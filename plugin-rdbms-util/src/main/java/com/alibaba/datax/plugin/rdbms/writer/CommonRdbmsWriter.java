package com.alibaba.datax.plugin.rdbms.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.SlavePluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class CommonRdbmsWriter {
	private static DataBaseType DATABASE_TYPE;
	private static final String VALUE_HOLDER = "?";

	public static class Master {
		private static final Logger LOG = LoggerFactory.getLogger(CommonRdbmsWriter.Master.class);

		private static final boolean IS_DEBUG = LOG.isDebugEnabled();

		public Master(DataBaseType dataBaseType) {
			DATABASE_TYPE = dataBaseType;
			OriginalConfPretreatmentUtil.DATABASE_TYPE = dataBaseType;
		}

		public void init(Configuration originalConfig) {
			OriginalConfPretreatmentUtil.doPretreatment(originalConfig);
			if (IS_DEBUG) {
				LOG.debug(
						"After master init(), originalConfig now is:[\n{}\n]",
						originalConfig.toJSON());
			}
		}

		// 一般来说，是需要推迟到 slave 中进行pre 的执行（单表情况例外）
		public void prepare(Configuration originalConfig) {
			int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK)
					.intValue();
			if (tableNumber == 1) {
				String username = originalConfig.getString(Key.USERNAME);
				String password = originalConfig.getString(Key.PASSWORD);

				List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
						Object.class);
				Configuration connConf = Configuration.from(conns.get(0)
						.toString());

				// 这里的 jdbcUrl 已经 append 了合适后缀参数
				String jdbcUrl = connConf.getString(Key.JDBC_URL);
				originalConfig.set(Key.JDBC_URL, jdbcUrl);

				String table = connConf.getList(Key.TABLE, String.class).get(0);
				originalConfig.set(Key.TABLE, table);

				List<String> preSqls = originalConfig.getList(Key.PRE_SQL,
						String.class);
				List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(
						preSqls, table);

				originalConfig.remove(Constant.CONN_MARK);
				if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
					// 说明有 preSql 配置，则此处删除掉
					originalConfig.remove(Key.PRE_SQL);

					Connection conn = DBUtil.getConnection(DATABASE_TYPE,
							jdbcUrl, username, password);
					LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
							StringUtils.join(renderedPreSqls, ";"), jdbcUrl);

					WriterUtil.executeSqls(conn, renderedPreSqls, jdbcUrl);
					DBUtil.closeDBResources(null, null, conn);
				}
			}

			if (IS_DEBUG) {
				LOG.debug(
						"After master prepare(), originalConfig now is:[\n{}\n]",
						originalConfig.toJSON());
			}
		}

		public List<Configuration> split(Configuration originalConfig,
				int mandatoryNumber) {
			return WriterUtil.doSplit(originalConfig, mandatoryNumber);
		}

        // 一般来说，是需要推迟到 slave 中进行post 的执行（单表情况例外）
        public void post(Configuration originalConfig) {
            int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK)
                    .intValue();
            if (tableNumber == 1) {
                String username = originalConfig.getString(Key.USERNAME);
                String password = originalConfig.getString(Key.PASSWORD);

                // 已经由 prepare 进行了appendJDBCSuffix处理
                String jdbcUrl = originalConfig.getString(Key.JDBC_URL);

                String table = originalConfig.getString(Key.TABLE);

                List<String> postSqls = originalConfig.getList(Key.POST_SQL,
                        String.class);
                List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(
                        postSqls, table);

                if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
                    // 说明有 postSql 配置，则此处删除掉
                    originalConfig.remove(Key.POST_SQL);

                    Connection conn = DBUtil.getConnection(DATABASE_TYPE,
                            jdbcUrl, username, password);

                    LOG.info(
                            "Begin to execute postSqls:[{}]. context info:{}.",
                            StringUtils.join(renderedPostSqls, ";"), jdbcUrl);
                    WriterUtil.executeSqls(conn, renderedPostSqls, jdbcUrl);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }
        }

		public void destroy(Configuration originalConfig) {
		}

	}

	public static class Slave {
		private static final Logger LOG = LoggerFactory
				.getLogger(CommonRdbmsWriter.Slave.class);

		private final static boolean IS_DEBUG = LOG.isDebugEnabled();

		private String username;

		private String password;

		private String jdbcUrl;

		private String table;

		private List<String> columns;

		private List<String> preSqls;

		private List<String> postSqls;

		private int batchSize;

		private int columnNumber = 0;

		private SlavePluginCollector slavePluginCollector;

		// 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
		private static String BASIC_MESSAGE;

		private static String INSERT_OR_REPLACE_TEMPLATE;

		private String writeRecordSql;

		private String writeMode;

		private Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

		public void init(Configuration writerSliceConfig) {
			this.username = writerSliceConfig.getString(Key.USERNAME);
			this.password = writerSliceConfig.getString(Key.PASSWORD);
			this.jdbcUrl = writerSliceConfig.getString(Key.JDBC_URL);
			this.table = writerSliceConfig.getString(Key.TABLE);

			this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
			this.columnNumber = this.columns.size();

			this.preSqls = writerSliceConfig.getList(Key.PRE_SQL, String.class);
			this.postSqls = writerSliceConfig.getList(Key.POST_SQL, String.class);
			this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);

			writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "INSERT");
			INSERT_OR_REPLACE_TEMPLATE = writerSliceConfig.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);
			this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);

			BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]",
					this.jdbcUrl, this.table);
		}

		public void prepare(Configuration writerSliceConfig) {
			Connection connection = DBUtil.getConnection(DATABASE_TYPE,
					this.jdbcUrl, username, password);

			DBUtil.dealWithSessionConfig(connection,
					writerSliceConfig.getList(Key.SESSION, String.class),
					DATABASE_TYPE, BASIC_MESSAGE);

			int tableNumber = writerSliceConfig.getInt(
					Constant.TABLE_NUMBER_MARK).intValue();
			if (tableNumber != 1) {
				LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
						StringUtils.join(this.preSqls, ";"), BASIC_MESSAGE);
				WriterUtil.executeSqls(connection, this.preSqls, BASIC_MESSAGE);
			}

			DBUtil.closeDBResources(null, null, connection);
		}

		// TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
		public void startWrite(RecordReceiver recordReceiver,
				Configuration writerSliceConfig,
				SlavePluginCollector slavePluginCollector) {
			this.slavePluginCollector = slavePluginCollector;

			Connection connection = DBUtil.getConnection(DATABASE_TYPE,
					this.jdbcUrl, username, password);
            DBUtil.dealWithSessionConfig(connection, writerSliceConfig,
                    DATABASE_TYPE, BASIC_MESSAGE);

			// 用于写入数据的时候的类型根据目的表字段类型转换
			this.resultSetMetaData = DBUtil.getColumnMetaData(connection,
					this.table, StringUtils.join(this.columns, ","));
			// 写数据库的SQL语句
			calcWriteRecordSql();

			List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
			try {
				Record record = null;
				while ((record = recordReceiver.getFromReader()) != null) {
					if (record.getColumnNumber() != this.columnNumber) {
						// 源头读取字段列数与目的表字段写入列数不相等，直接报错
						throw DataXException
								.asDataXException(
										DBUtilErrorCode.CONF_ERROR,
										String.format(
												"您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置字段.",
												record.getColumnNumber(),
												this.columnNumber));
					}

					writeBuffer.add(record);

					if (writeBuffer.size() >= batchSize) {
						doBatchInsert(connection, writeBuffer);
						writeBuffer.clear();
					}
				}
				if (!writeBuffer.isEmpty()) {
					doBatchInsert(connection, writeBuffer);
					writeBuffer.clear();
				}
			} catch (Exception e) {
				throw DataXException.asDataXException(
						DBUtilErrorCode.WRITE_DATA_ERROR, e);
			} finally {
				writeBuffer.clear();
				DBUtil.closeDBResources(null, null, connection);
			}
		}

        public void post(Configuration writerSliceConfig) {
            int tableNumber = writerSliceConfig.getInt(
                    Constant.TABLE_NUMBER_MARK).intValue();

            boolean hasPostSql = (this.postSqls != null && this.postSqls.size() > 0);
            if (tableNumber == 1 || !hasPostSql) {
                return;
            }

            Connection connection = DBUtil.getConnection(DATABASE_TYPE,
                    this.jdbcUrl, username, password);

            LOG.info("Begin to execute postSqls:[{}]. context info:{}.",
                    StringUtils.join(this.postSqls, ";"), BASIC_MESSAGE);
            WriterUtil.executeSqls(connection, this.postSqls, BASIC_MESSAGE);
            DBUtil.closeDBResources(null, null, connection);
        }

		public void destroy(Configuration writerSliceConfig) {
		}

		private void doBatchInsert(Connection connection, List<Record> buffer)
				throws SQLException {
			PreparedStatement preparedStatement = null;
			try {
				connection.setAutoCommit(false);
				preparedStatement = connection
						.prepareStatement(this.writeRecordSql);

				for (Record record : buffer) {
					preparedStatement = fillPreparedStatement(
							preparedStatement, record);
					preparedStatement.addBatch();
				}
				preparedStatement.executeBatch();
				connection.commit();
			} catch (SQLException e) {
				LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
				connection.rollback();
				doOneInsert(connection, buffer);
			} catch (Exception e) {
				throw DataXException.asDataXException(
						DBUtilErrorCode.WRITE_DATA_ERROR, e);
			} finally {
				DBUtil.closeDBResources(preparedStatement, null);
			}
		}

		private void doOneInsert(Connection connection, List<Record> buffer) {
			PreparedStatement preparedStatement = null;
			try {
				connection.setAutoCommit(true);
				preparedStatement = connection
						.prepareStatement(this.writeRecordSql);

				for (Record record : buffer) {
					try {
						preparedStatement = fillPreparedStatement(
								preparedStatement, record);
						preparedStatement.execute();
					} catch (SQLException e) {
						if (IS_DEBUG) {
							LOG.debug(e.toString());
						}

						this.slavePluginCollector.collectDirtyRecord(record, e);
					} finally {
						// 最后不要忘了关闭 preparedStatement
						preparedStatement.clearParameters();
					}
				}
			} catch (Exception e) {
				throw DataXException.asDataXException(
						DBUtilErrorCode.WRITE_DATA_ERROR, e);
			} finally {
				DBUtil.closeDBResources(preparedStatement, null);
			}
		}

		// 直接使用了两个类变量：columnNumber,resultSetMetaData
		private PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record)
				throws SQLException {
			java.util.Date utilDate = null;
			for (int i = 0; i < this.columnNumber; i++) {

				switch (this.resultSetMetaData.getMiddle().get(i)) {
				case Types.CHAR:
				case Types.NCHAR:
				case Types.CLOB:
				case Types.NCLOB:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.NVARCHAR:
				case Types.LONGNVARCHAR:
				case Types.SMALLINT:
				case Types.TINYINT:
				case Types.INTEGER:
				case Types.BIGINT:
				case Types.NUMERIC:
				case Types.DECIMAL:
				case Types.FLOAT:
				case Types.REAL:
				case Types.DOUBLE:
				case Types.BIT:
					preparedStatement.setString(i + 1, record.getColumn(i)
							.asString());
					break;

				// for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
				case Types.DATE:
					if (this.resultSetMetaData.getRight().get(i)
							.equalsIgnoreCase("year")) {
						preparedStatement.setString(i + 1, record.getColumn(i)
								.asString());
					} else {
						java.sql.Date sqlDate = null;
						try {
							utilDate = record.getColumn(i).asDate();
						} catch (DataXException e) {
							throw new SQLException(String.format(
									"Date 类型转换错误：[%s]", record.getColumn(i)));
						}

						if (null != utilDate) {
							sqlDate = new java.sql.Date(utilDate.getTime());
						}
						preparedStatement.setDate(i + 1, sqlDate);
					}
					break;

				case Types.TIME:
					java.sql.Time sqlTime = null;
					try {
						utilDate = record.getColumn(i).asDate();
					} catch (DataXException e) {
						throw new SQLException(String.format(
								"TIME 类型转换错误：[%s]", record.getColumn(i)));
					}

					if (null != utilDate) {
						sqlTime = new java.sql.Time(utilDate.getTime());
					}
					preparedStatement.setTime(i + 1, sqlTime);
					break;

				case Types.TIMESTAMP:
					java.sql.Timestamp sqlTimestamp = null;
					try {
						utilDate = record.getColumn(i).asDate();
					} catch (DataXException e) {
						throw new SQLException(String.format(
								"TIMESTAMP 类型转换错误：[%s]", record.getColumn(i)));
					}

					if (null != utilDate) {
						sqlTimestamp = new java.sql.Timestamp(
								utilDate.getTime());
					}
					preparedStatement.setTimestamp(i + 1, sqlTimestamp);
					break;

				case Types.BINARY:
				case Types.VARBINARY:
				case Types.BLOB:
				case Types.LONGVARBINARY:
					preparedStatement.setBytes(i + 1, record.getColumn(i)
							.asBytes());
					break;
				case Types.BOOLEAN:
					preparedStatement.setBoolean(i + 1, record.getColumn(i)
							.asBoolean());
					break;
				default:
					throw DataXException
							.asDataXException(
									DBUtilErrorCode.UNSUPPORTED_TYPE,
									String.format(
											"DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s].",
											this.resultSetMetaData.getLeft()
													.get(i),
											this.resultSetMetaData.getMiddle()
													.get(i),
											this.resultSetMetaData.getRight()
													.get(i)));
				}
			}

			return preparedStatement;
		}

		private void calcWriteRecordSql(){
			if(!VALUE_HOLDER.equals(calcValueHolder(""))){
				List<String> valueHolders = new ArrayList<String>(columnNumber);
				for (int i = 0; i < columns.size(); i++) {
					String type = resultSetMetaData.getRight().get(i);
					valueHolders.add(calcValueHolder(type));
				}
				INSERT_OR_REPLACE_TEMPLATE = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode);
				writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);
			}
		}

		protected String calcValueHolder(String columnType){
			return VALUE_HOLDER;
		}
	}
}
