package com.alibaba.datax.plugin.rdbms.reader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.ReaderSplitUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.SqlFormatUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

public class CommonRdbmsReader {

	private static DataBaseType DATABASE_TYPE;

	public static class Job {
		private static final Logger LOG = LoggerFactory
				.getLogger(Job.class);

		private static final boolean IS_DEBUG = LOG.isDebugEnabled();

		public Job(DataBaseType dataBaseType) {
			DATABASE_TYPE = dataBaseType;
			OriginalConfPretreatmentUtil.DATABASE_TYPE = dataBaseType;
			SingleTableSplitUtil.DATABASE_TYPE = dataBaseType;
		}

		public void init(Configuration originalConfig) {

			OriginalConfPretreatmentUtil.doPretreatment(originalConfig);

			if (IS_DEBUG) {
				LOG.debug("After master init, job config now is:[\n{}\n]",
						originalConfig.toJSON());
			}
		}

		public List<Configuration> split(Configuration originalConfig,
				int adviceNumber) {
			return ReaderSplitUtil.doSplit(originalConfig, adviceNumber);
		}

		public void post(Configuration originalConfig) {
			// do nothing
		}

		public void destroy(Configuration originalConfig) {
			// do nothing
		}

	}

	public static class Task {
		private static final Logger LOG = LoggerFactory
				.getLogger(Task.class);

		private String username;
		private String password;
		private String jdbcUrl;

		// 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
		private static String BASIC_MESSAGE;

		public Task(DataBaseType dataBaseType) {
			DATABASE_TYPE = dataBaseType;
		}

		public void init(Configuration readerSliceConfig) {

			/* for database connection */

			this.username = readerSliceConfig.getString(Key.USERNAME);
			this.password = readerSliceConfig.getString(Key.PASSWORD);
			this.jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);

			BASIC_MESSAGE = String.format("jdbcUrl:[%s]", this.jdbcUrl);
		}

		//TODO: 详细区分错误原因，代码可能有bug
		public void startRead(Configuration readerSliceConfig,
				RecordSender recordSender,
				TaskPluginCollector taskPluginCollector, int fetchSize) {
			String querySql = readerSliceConfig.getString(Key.QUERY_SQL);
			String formattedSql = null;

			try {
				formattedSql = SqlFormatUtil.format(querySql);
			} catch (Exception unused) {
				// ignore it
			}
			LOG.info("Begin to read record by Sql [{}\n] {}.",
					null != formattedSql ? formattedSql : querySql,
					BASIC_MESSAGE);

			Connection conn = DBUtil.getConnection(DATABASE_TYPE, jdbcUrl,
					username, password);

			// session config .etc related
			DBUtil.dealWithSessionConfig(conn, readerSliceConfig,
					DATABASE_TYPE, BASIC_MESSAGE);

			int columnNumber = 0;
			ResultSet rs = null;
			try {
				rs = DBUtil.query(conn, querySql, fetchSize);
				ResultSetMetaData metaData = rs.getMetaData();
				columnNumber = metaData.getColumnCount();

				while (rs.next()) {
					ResultSetReadProxy.transportOneRecord(recordSender, rs,
							metaData, columnNumber, taskPluginCollector);
				}
			} catch (Exception e) {
				throw DataXException.asDataXException(
						DBUtilErrorCode.READ_RECORD_FAIL, String.format(
								"读取数据库失败. 因为根据您配置的相关上下文信息:%s , 执行的语句:[%s]失败. 请检查您的配置并作出修改或者.",
								BASIC_MESSAGE, querySql), e);
			} finally {
				DBUtil.closeDBResources(null, conn);
			}
		}

		public void post(Configuration originalConfig) {
			// do nothing
		}

		public void destroy(Configuration originalConfig) {
			// do nothing
		}
	}
}
