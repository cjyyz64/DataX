package com.alibaba.datax.plugin.reader.sqlserverreader;

<<<<<<< HEAD
import com.alibaba.datax.common.element.*;
=======
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
>>>>>>> 13de4399a087463ea60bc5384d52025e0605e742
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.SqlFormatUtil;
import com.alibaba.datax.plugin.reader.sqlserverreader.util.ConfigPretreatUtil;
import com.alibaba.datax.plugin.reader.sqlserverreader.util.TableSplitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public class SqlServerReader {
	public static class Master extends Reader.Master {
		private static final Logger LOG = LoggerFactory
				.getLogger(SqlServerReader.Master.class);

		private Configuration readerOriginConfig;

		@Override
		public void init() {
			LOG.info("init() begin ...");
			this.readerOriginConfig = this.getPluginJobConf();
			ConfigPretreatUtil.validate(this.readerOriginConfig);
			LOG.info("init() end ...");
		}

		@Override
		public void prepare() {
			LOG.info("prepare()");
		}

		@Override
		public void post() {
			LOG.info("post()");
		}

		@Override
		public void destroy() {
			LOG.info("destroy()");
		}

		@Override
		public List<Configuration> split(int adviceNumber) {
			LOG.info("split()");
			return TableSplitUtil
					.doSplit(this.readerOriginConfig, adviceNumber);
		}

	}

	public static class Slave extends Reader.Slave {
		private static final Logger LOG = LoggerFactory
				.getLogger(SqlServerReader.Slave.class);

		private Configuration readerSliceConfig;

		private String jdbcUrl;

		private String username;

		private String password;

		private int fetchSize;

		@Override
		public void init() {
			LOG.info("init()");
			this.readerSliceConfig = this.getPluginJobConf();

			this.username = this.readerSliceConfig.getString(Key.USERNAME);
			this.password = this.readerSliceConfig.getString(Key.PASSWORD);
			this.jdbcUrl = this.readerSliceConfig.getString(Key.JDBC_URL);

			this.fetchSize = this.readerSliceConfig.getInt(Key.FETCH_SIZE);

		}

		@Override
		public void prepare() {
			LOG.info("prepare()");
		}

		@Override
		public void post() {
			LOG.info("post()");
		}

		@Override
		public void destroy() {
			LOG.info("destroy()");
		}

		@Override
		public void startRead(RecordSender recordSender) {
			String sql = this.readerSliceConfig.getString(Key.QUERYSQL);

			String formatedSql = null;

			try {
				formatedSql = SqlFormatUtil.format(sql);
			} catch (Exception unused) {
				// ignore it
			}

			LOG.info("do query \n[{}\n]\n, from jdbc-url:\n[{}]\n",
					null != formatedSql ? formatedSql : sql, this.jdbcUrl);

			ResultSet rs = null;
			Connection conn = DBUtil.getConnection(DataBaseType.SQLServer, this.jdbcUrl,
					this.username, this.password);

			try {
				rs = DBUtil.query(conn, sql, this.fetchSize);

				while (rs.next()) {
					this.transportOneRecord(recordSender, rs);
				}
			} catch (SQLException e) {
				throw new DataXException(
						SqlServerReaderErrorCode.RUNTIME_EXCEPTION, e);
			} finally {
				DBUtil.closeDBResources(rs, null, conn);
			}

		}

		private void transportOneRecord(RecordSender recordSender, ResultSet rs) {
			ResultSetMetaData metaData;
			int columnNumber;
			Record record = recordSender.createRecord();
			try {
				metaData = rs.getMetaData();
				columnNumber = metaData.getColumnCount();

				// warn:begin from 1
				for (int i = 1; i <= columnNumber; i++) {
					switch (metaData.getColumnType(i)) {

					case Types.CHAR:
					case Types.NCHAR:
					case Types.CLOB:
					case Types.NCLOB:
					case Types.VARCHAR:
					case Types.LONGVARCHAR:
					case Types.NVARCHAR:
					case Types.LONGNVARCHAR:
						record.addColumn(new StringColumn(rs.getString(i)));
						break;

					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.INTEGER:
					case Types.BIGINT:
						record.addColumn(new LongColumn(rs.getInt(i)));
						break;

					case Types.NUMERIC:
					case Types.DECIMAL:
						record.addColumn(new DoubleColumn(rs.getString(i)));
						break;

					case Types.FLOAT:
					case Types.REAL:
					case Types.DOUBLE:
						record.addColumn(new DoubleColumn(rs.getDouble(i)));
						break;

					case Types.DATE:
						record.addColumn(new DateColumn(rs.getDate(i)));
						break;
					case Types.TIMESTAMP:
						record.addColumn(new DateColumn(rs.getTimestamp(i)));
						break;
					case Types.TIME:
						record.addColumn(new DateColumn(rs.getTime(i)));
						break;

					case Types.BINARY:
					case Types.VARBINARY:
					case Types.LONGVARBINARY:
					case Types.BLOB:
						record.addColumn(new BytesColumn(rs.getBytes(i)));
						break;

					// warn:BIT
					case Types.BIT:
					case Types.BOOLEAN:
						record.addColumn(new BoolColumn(rs.getBoolean(i)));
						break;

					default:
						throw new Exception(
								String.format(
										"unsupport SqlServer Data Type. ColumnName:[%s], ColumnType:[%s], ColumnClassName:[%s].",
										metaData.getColumnName(i),
										metaData.getColumnType(i),
										metaData.getColumnClassName(i)));
					}
				}

				recordSender.sendToWriter(record);

			} catch (SQLException e) {
				// 此异常不是脏数据
				throw new DataXException(
						SqlServerReaderErrorCode.RUNTIME_EXCEPTION,
						"unable to get meta data.");
			} catch (Exception e) {
				// TODO 脏数据处理
				this.getSlavePluginCollector().collectDirtyRecord(record,
						e.getMessage());
				// throw new DataXException(
				// SqlServerReaderErrorCode.RUNTIME_EXCEPTION,
				// e.getMessage(), e);
			}
		}
	}

}
