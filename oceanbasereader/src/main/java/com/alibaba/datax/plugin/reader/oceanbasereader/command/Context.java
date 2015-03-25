package com.alibaba.datax.plugin.reader.oceanbasereader.command;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.ResultSetReadProxy;
import com.alibaba.datax.plugin.reader.oceanbasereader.Index;
import com.alibaba.datax.plugin.reader.oceanbasereader.Key;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.SelectExpression;
import com.alibaba.datax.plugin.reader.oceanbasereader.parser.SelectParser;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.*;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
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
        this.setTabletIfnotExist();
	}

    private void setTabletIfnotExist() {
        try {
            if(this.lowVersion()) return;
            RowkeyMeta meta = this.rowkeyMeta();
            List<?> startkey = configuration.getList("startkey");
            List<?> endkey = configuration.getList("endkey");
            if(startkey == null || endkey == null){
                configuration.set("tablet", new Tablet(meta, ImmutableList.of(RowkeyMeta.BoundValue.OB_MIN),ImmutableList.of(RowkeyMeta.BoundValue.OB_MAX)));
            }else {
                configuration.set("tablet", new Tablet(meta,startkey,endkey));
            }

        }catch (Exception e){
            throw new RuntimeException("fetch rowkey info error",e);
        }
    }

    private RowkeyMeta rowkeyMeta() throws Exception{
        String url = this.url();
        String table = this.table();
        String tableId = JDBCDataSource.tableId(url, table);
        return JDBCDataSource.rowkey(url, table, tableId);
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
		while (result.next()) {
			ResultSetReadProxy.transportOneRecord(recordSender, result, metaData, columnNumber, null, slavePluginCollector);
		}
	}

	public boolean lowVersion() throws Exception {
		return OBDataSource.execute(this.url(), OBVersionHandler.version, new OBVersionHandler());
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
		return OBDataSource.execute(this.url(),String.format("desc %s", table()), handler);
	}

    public String url(){
        return configuration.getString(Key.CONFIG_URL);
    }

    public int limit(){
        return configuration.getInt(Key.FETCH_SIZE, 1000);
    }

    public Tablet tablet(){
        return (Tablet)configuration.get("tablet");
    }
}