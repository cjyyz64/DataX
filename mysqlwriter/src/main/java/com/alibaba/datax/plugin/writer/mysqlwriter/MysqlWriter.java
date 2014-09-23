package com.alibaba.datax.plugin.writer.mysqlwriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.NumberColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.writer.mysqlwriter.util.MysqlWriterSplitUtil;
import com.alibaba.datax.plugin.writer.mysqlwriter.util.OriginalConfPretreatmentUtil;

public class MysqlWriter extends Writer {

	public static class Master extends Writer.Master {
		private static final Logger LOG = LoggerFactory
				.getLogger(MysqlWriter.Master.class);

		private Configuration originalConfig = null;

		@Override
		public void init() {
			LOG.info("init() begin ...");
			this.originalConfig = getPluginJobConf();
			OriginalConfPretreatmentUtil.doPretreatment(this.originalConfig);
			LOG.info("init() end ...");
		}

		// 一般来说，是需要推迟到 slave 中进行pre 的执行
		@Override
		public void prepare() {
			LOG.info("prepare() begin ...");
			LOG.info("prepare() end ...");
		}

		@Override
		public List<Configuration> split(int adviceNumber) {
			return MysqlWriterSplitUtil.doSplit(this.originalConfig,
					adviceNumber);
		}

		// 一般来说，是需要推迟到 slave 中进行post 的执行
		@Override
		public void post() {
			LOG.info("post() begin ...");
			LOG.info("post() end ...");
		}

		@Override
		public void destroy() {
			LOG.info("destroy() begin ...");
			LOG.info("destroy() end ...");
		}

	}

	public static class Slave extends Writer.Slave {
		private static final Logger LOG = LoggerFactory
				.getLogger(MysqlWriter.Slave.class);

		private final static boolean IS_DEBUG = LOG.isDebugEnabled();

		private Configuration writerSliceConfig;
		private String username;
		private String password;
		private String jdbcUrl;
		private String table;
		private List<String> preSqls;
		private List<String> postSqls;
		private int batchSize;

		private Connection conn;

		private int columnNumber = 0;

		// 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
		private static String BASIC_MESSAGE;

		private static String INSERT_OR_REPLACE_TEMPLATE;

		@Override
		public void init() {
			this.writerSliceConfig = getPluginJobConf();
			this.username = this.writerSliceConfig.getString(Key.USERNAME);
			this.password = this.writerSliceConfig.getString(Key.PASSWORD);
			this.jdbcUrl = this.writerSliceConfig.getString(Key.JDBC_URL);
			this.table = this.writerSliceConfig.getString(Key.TABLE);

			this.columnNumber = this.writerSliceConfig
					.getInt(Constant.COLUMN_NUMBER_MARK);

			this.preSqls = this.writerSliceConfig.getList(Key.PRE_SQL,
					String.class);
			this.postSqls = this.writerSliceConfig.getList(Key.POST_SQL,
					String.class);
			this.batchSize = this.writerSliceConfig.getInt(Key.BATCH_SIZE, 32);

			this.conn = DBUtil.getConnection("mysql", this.jdbcUrl, username,
					password);

			INSERT_OR_REPLACE_TEMPLATE = this.writerSliceConfig
					.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);

			BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]",
					this.jdbcUrl, this.table);
		}

		@Override
		public void prepare() {
			dealSessionConf(conn,
					this.writerSliceConfig.getMap(Key.SESSION, String.class));

			executeSqls(this.conn, this.preSqls);
		}

		public void startWrite(RecordReceiver recordReceiver) {
			List<Record> array = new ArrayList<Record>(this.batchSize);
			try {
				Record record = null;
				while ((record = recordReceiver.getFromReader()) != null) {
					array.add(record);
					if (array.size() >= batchSize) {
						doBatchInsert(conn, table, array);
					}
				}
				if (0 != array.size()) {
					doBatchInsert(conn, table, array);
				}
			} catch (Exception e) {
				throw new DataXException(MysqlWriterErrorCode.UNKNOWN_ERROR, e);
			}

		}

		@Override
		public void post() {
			executeSqls(this.conn, this.postSqls);
		}

		@Override
		public void destroy() {
			LOG.info("destroy() begin ...");
			LOG.info("destroy() end ...");
		}

		private void executeSqls(Connection conn, List<String> sqls) {
			Statement stmt;
			try {
				stmt = conn.createStatement();
			} catch (SQLException e1) {
				throw new DataXException(MysqlWriterErrorCode.UNKNOWN_ERROR, e1);
			}

			for (String sql : sqls) {
				try {
					DBUtil.executeSqlWithoutResultSet(stmt, sql);
				} catch (SQLException e2) {
					LOG.error("execute sql:[{}] failed, {}.", sql,
							BASIC_MESSAGE);
					throw new DataXException(
							MysqlWriterErrorCode.UNKNOWN_ERROR, e2);
				}
			}
		}

		// TODO 类型转换的 review
		private void doBatchInsert(Connection connection, String table,
				List<Record> buffer) throws SQLException {

			PreparedStatement pstmt = null;
			try {
				connection.setAutoCommit(false);
				pstmt = connection.prepareStatement(String.format(
						INSERT_OR_REPLACE_TEMPLATE, table));

				for (Record record : buffer) {
					for (int i = 0; i < this.columnNumber; i++) {
						pstmt = buildPreparedStatement(record.getColumn(i),
								pstmt, i + 1);
					}
					pstmt.addBatch();
				}
				pstmt.executeBatch();
				connection.commit();
			} catch (Exception e) {
				connection.rollback();
				doBadInsert(connection, table, buffer);
			} finally {
				DBUtil.closeDBResources(pstmt, null);
				buffer.clear();
			}
		}

		// TODO 检查columnNumber 和 record.getColumnNumber() 大小
		private void doBadInsert(Connection connection, String table,
				List<Record> buffer) {
			PreparedStatement pstmt = null;
			try {
				connection.setAutoCommit(true);
				for (Record record : buffer) {
					try {
						pstmt = connection.prepareStatement(String.format(
								INSERT_OR_REPLACE_TEMPLATE, table));
						for (int i = 0; i < this.columnNumber; i++) {
							pstmt = buildPreparedStatement(record.getColumn(i),
									pstmt, i + 1);
						}

						pstmt.execute();
						pstmt.close();
					} catch (Exception e) {
						if (IS_DEBUG) {
							LOG.debug(e.toString());
						}

						this.getSlavePluginCollector().collectDirtyRecord(
								record, e);
					} finally {
						DBUtil.closeDBResources(pstmt, null);
					}
				}
			} catch (Exception e) {
				throw new DataXException(MysqlWriterErrorCode.UNKNOWN_ERROR, e);
			} finally {
				DBUtil.closeDBResources(pstmt, null);
				buffer.clear();
			}
		}

		private static String SESSION_TEMPLATE = "SET %s=%s;";

		private static void dealSessionConf(Connection conn,
				Map<String, String> sessionConfs) {
			Statement stmt;
			try {
				stmt = conn.createStatement();
			} catch (SQLException e) {
				LOG.error("error while createStatement, {}.", BASIC_MESSAGE);
				throw new DataXException(MysqlWriterErrorCode.UNKNOWN_ERROR, e);
			}

			String sessionSql = null;
			for (Entry<String, String> entry : sessionConfs.entrySet()) {
				sessionSql = String.format(SESSION_TEMPLATE, entry.getKey(),
						entry.getValue());

				LOG.info("execute sql:[{}]", sessionSql);
				try {
					DBUtil.executeSqlWithoutResultSet(stmt, sessionSql);
				} catch (SQLException e) {
					LOG.error("execute sql:[{}] failed, {}.", sessionSql,
							BASIC_MESSAGE);
					throw new DataXException(
							MysqlWriterErrorCode.UNKNOWN_ERROR, e);
				}
			}

			DBUtil.closeDBResources(stmt, null);
		}

		private PreparedStatement buildPreparedStatement(Column tempColumn,
				PreparedStatement pstmt, int index) throws Exception {
			if (tempColumn instanceof StringColumn
					|| tempColumn instanceof NumberColumn) {
				pstmt.setString(index, tempColumn.toString());
			} else if (tempColumn instanceof BytesColumn) {
				pstmt.setBytes(index, tempColumn.asBytes());
			} else if (tempColumn instanceof DateColumn) {
				pstmt.setObject(index, tempColumn.asDate());
			} else if (tempColumn instanceof BoolColumn) {
				pstmt.setBoolean(index, tempColumn.asBoolean());
			}
			return pstmt;
		}

	}

}
