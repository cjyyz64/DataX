package com.alibaba.datax.plugin.reader.oceanbasereader.command;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.ResultSetReadProxy;
import com.alibaba.datax.plugin.reader.oceanbasereader.Index;
import com.alibaba.datax.plugin.reader.oceanbasereader.Key;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.SelectExpression;
import com.alibaba.datax.plugin.reader.oceanbasereader.parser.SelectParser;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.OBDataSource;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.ResultSetHandler;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public class Context {

	private final Configuration configuration;
    private final RecordSender recordSender;
    private final TaskPluginCollector slavePluginCollector;

	public Context(Configuration configuration, RecordSender recordSender, TaskPluginCollector slavePluginCollector) {
		this.configuration = configuration;
        this.recordSender = recordSender;
        this.slavePluginCollector = slavePluginCollector;
	}

	private String table() {
		return this.orginalAst().table;
	}

	public SelectExpression orginalAst() {
		return SelectParser.parse(this.originalSQL());
	}

    private static final Joiner joiner = Joiner.on(",");
	public String originalSQL() {
		String customSQL = configuration.getString(Key.SQL, "");
		if (StringUtils.isNotBlank(customSQL))
			return customSQL;
		String table = configuration.getString(Key.TABLE);
		List<String> columns = configuration.getList(Key.COLUMN, String.class);
		String where = configuration.getString(Key.WHERE, "");
		String sqlTemplate = "SELECT %s FROM %s %s";
		return String.format(sqlTemplate, joiner.join(columns), table,
				(Strings.isNullOrEmpty(where) ? "" : " where " + where));
	}

	public void sendToWriter(ResultSet result) throws SQLException {
        ResultSetMetaData metaData = result.getMetaData();
        int columnNumber = metaData.getColumnCount();
        while (result.next()){
            ResultSetReadProxy.transportOneRecord(recordSender, result, metaData, columnNumber, slavePluginCollector);
        }
	}

	public int timeout(){
		return configuration.getInt(Key.TIMEOUT, 5) * 60 * 60 * 1000 * 1000;
	}
	
	public boolean lowVersion() throws Exception {
		ResultSetHandler<Boolean> handler = new ResultSetHandler<Boolean>() {
			@Override
			public Boolean callback(ResultSet result) throws SQLException {
				result.next();
				String version = result.getString("value");
				return version.contains("0.4");
			}
		};
		return OBDataSource.execute(this.url(), "show variables like 'version_comment'",
                this.timeout(), handler);
	}

	public Index rowkey() throws Exception {
		ResultSetHandler<Index> handler = new ResultSetHandler<Index>() {

			@Override
			public Index callback(ResultSet result) throws Exception {
				Index.Builder builder = Index.builder();
				while (result.next()) {
					String name = result.getString("field");
					String type = result.getString("type");
					int key = result.getInt("key");
					if (key != 0)
						builder.addEntry(name, type, key);
				}
				return builder.build();
			}
		};
		return OBDataSource.execute(this.url(),String.format("desc %s", table()), this.timeout(), handler);
	}

	private Map<String, String> columnAndType() throws Exception {
		ResultSetHandler<Map<String, String>> handler = new ResultSetHandler<Map<String, String>>() {
			@Override
			public Map<String, String> callback(ResultSet result)
					throws Exception {
				Map<String, String> map = new HashMap<String, String>();
				while (result.next()) {
					String name = result.getString("field");
					String type = result.getString("type");
					map.put(name, type);
				}
				return map;
			}
		};
		return OBDataSource.execute(this.url(),String.format("desc %s", table()), this.timeout(), handler);
	}

	public Set<Index> secondaryIndex() throws Exception {
		final Map<String, String> meta = columnAndType();
		ResultSetHandler<Set<Index>> handler = new ResultSetHandler<Set<Index>>() {

			@Override
			public Set<Index> callback(ResultSet result) throws Exception {
				Map<String, Index.Builder> map = new HashMap<String, Index.Builder>();
				Index.Builder builder;
				while (result.next()) {
					String indexName = result.getString("key_name");
					if (map.containsKey(indexName)) {
						builder = map.get(indexName);
					} else {
						builder = Index.builder();
						map.put(indexName, builder);
					}
					String name = result.getString("column_name");
					String type = meta.get(name);
					int position = result.getInt("seq_in_index");
					builder.addEntry(name, type, position);
				}
				Set<Index> indexes = new HashSet<Index>(map.size());
				for (Map.Entry<String, Index.Builder> entry : map.entrySet()) {
					indexes.add(entry.getValue().build());
				}
				return indexes;
			}
		};
		return OBDataSource.execute(
				this.url(), String.format("show index from %s", table()), timeout(), handler);
	}

	public Set<Index> index() throws Exception {
		return ImmutableSet.<Index> builder().add(rowkey())
				.addAll(secondaryIndex()).build();
	}

    public String url(){
        return configuration.getString(Key.CONFIG_URL);
    }

    public int limit(){
        return configuration.getInt(Key.FETCH_SIZE, 1000);
    }
}