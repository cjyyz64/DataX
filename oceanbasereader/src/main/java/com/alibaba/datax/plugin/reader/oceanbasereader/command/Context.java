package com.alibaba.datax.plugin.reader.oceanbasereader.command;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.reader.oceanbasereader.Index;
import com.alibaba.datax.plugin.reader.oceanbasereader.Key;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.SelectExpression;
import com.alibaba.datax.plugin.reader.oceanbasereader.parser.SelectParser;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.*;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
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
    
    static class ResultSetReadProxy {
        private static final Logger LOG = LoggerFactory
                .getLogger(ResultSetReadProxy.class);

        private static final boolean IS_DEBUG = LOG.isDebugEnabled();
        private static final byte[] EMPTY_CHAR_ARRAY = new byte[0];

        public static void transportOneRecord(RecordSender recordSender, ResultSet rs, 
                ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding, 
                TaskPluginCollector taskPluginCollector) {
            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGNVARCHAR:
                        String rawData;
                        if(StringUtils.isBlank(mandatoryEncoding)){
                            rawData = rs.getString(i);
                        }else{
                            rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY : 
                                rs.getBytes(i)), mandatoryEncoding);
                        }
                        record.addColumn(new StringColumn(rawData));
                        break;

                    case Types.CLOB:
                    case Types.NCLOB:
                        record.addColumn(new StringColumn(rs.getString(i)));
                        break;

                    case Types.SMALLINT:
                    case Types.TINYINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        record.addColumn(new LongColumn(rs.getString(i)));
                        break;

                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;

                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;

                    case Types.TIME:
                        record.addColumn(new DateColumn(rs.getTime(i)));
                        break;

                    // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                    case Types.DATE:
                        if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                            record.addColumn(new LongColumn(rs.getInt(i)));
                        } else {
                            record.addColumn(new DateColumn(rs.getDate(i)));
                        }
                        break;

                    case Types.TIMESTAMP:
                        record.addColumn(new DateColumn(rs.getTimestamp(i)));
                        break;

                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.BLOB:
                    case Types.LONGVARBINARY:
                        record.addColumn(new BytesColumn(rs.getBytes(i)));
                        break;

                    // warn: bit(1) -> Types.BIT 可使用BoolColumn
                    // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                    case Types.BOOLEAN:
                    case Types.BIT:
                        record.addColumn(new BoolColumn(rs.getBoolean(i)));
                        break;

                    case Types.NULL:
                        String stringData = null;
                        if(rs.getObject(i) != null) {
                            stringData = rs.getObject(i).toString();
                        }
                        record.addColumn(new StringColumn(stringData));
                        break;

                    default:
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.UNSUPPORTED_TYPE,
                                        String.format(
                                                "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                metaData.getColumnName(i),
                                                metaData.getColumnType(i),
                                                metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("read data " + record.toString()
                            + " occur exception:", e);
                }
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }

            recordSender.sendToWriter(record);
        }
    }
}