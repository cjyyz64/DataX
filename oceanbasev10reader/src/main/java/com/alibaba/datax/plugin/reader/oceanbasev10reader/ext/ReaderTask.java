package com.alibaba.datax.plugin.reader.oceanbasev10reader.ext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.Config;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.OBUtils;

public class ReaderTask extends CommonRdbmsReader.Task {
	private static final Logger LOG = LoggerFactory.getLogger(ReaderTask.class);
	private int taskGroupId = -1;
	private int taskId = -1;

	private String username;
	private String password;
	private String jdbcUrl;
	private String mandatoryEncoding;
	private int queryTimeoutSeconds;//

	public ReaderTask(int taskGropuId, int taskId) {
		super(OBUtils.DATABASE_TYPE, taskGropuId, taskId);
		this.taskGroupId = taskGropuId;
		this.taskId = taskId;
	}

	public void init(Configuration readerSliceConfig) {
		/* for database connection */
		this.username = readerSliceConfig.getString(Key.USERNAME);
		this.password = readerSliceConfig.getString(Key.PASSWORD);
		this.jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);
		this.queryTimeoutSeconds = readerSliceConfig.getInt(Config.QUERY_TIMEOUT_SECOND,
				Config.DEFAULT_QUERY_TIMEOUT_SECOND);

		String[] ss = this.jdbcUrl.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
		if (ss.length != 3) {
			throw DataXException.asDataXException(DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
		}
		LOG.info("this is ob1_0 jdbc url.");
		this.username = ss[1].trim() + ":" + this.username;
		this.jdbcUrl = ss[2];
		LOG.info("this is ob1_0 jdbc url. user=" + this.username + " :url=" + this.jdbcUrl);

		this.mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "");
	}

	@Override
	public void startRead(Configuration readerSliceConfig, RecordSender recordSender,
			TaskPluginCollector taskPluginCollector, int fetchSize) {
		String basicMsg = String.format("jdbcUrl:[%s]", this.jdbcUrl);
		String querySql = readerSliceConfig.getString(Key.QUERY_SQL);
		String table = readerSliceConfig.getString(Key.TABLE);

		PerfTrace.getInstance().addTaskDetails(taskId, table + "," + basicMsg);

		LOG.info("Begin to read record by Sql: [{}\n] {}.", querySql, basicMsg);
		PerfRecord queryPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.SQL_QUERY);
		queryPerfRecord.start();

		Connection conn = DBUtil.getConnection(OBUtils.DATABASE_TYPE, jdbcUrl, username, password);
		// session config .etc related
//		DBUtil.dealWithSessionConfig(conn, readerSliceConfig, OBUtils.DATABASE_TYPE, basicMsg);
		// OceanBase special session config
		OBUtils.initConn4Reader(conn, queryTimeoutSeconds);

		int columnNumber = 0;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(fetchSize);
			stmt.setQueryTimeout(queryTimeoutSeconds);
			rs = stmt.executeQuery(querySql);
			queryPerfRecord.end();

			ResultSetMetaData metaData = rs.getMetaData();
			columnNumber = metaData.getColumnCount();

			// 这个统计干净的result_Next时间
			PerfRecord allResultPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
			allResultPerfRecord.start();

			long rsNextUsedTime = 0;
			long lastTime = System.nanoTime();
			while (rs.next()) {
				rsNextUsedTime += (System.nanoTime() - lastTime);
				this.transportOneRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding,
						taskPluginCollector);
				lastTime = System.nanoTime();
			}

			allResultPerfRecord.end(rsNextUsedTime);
			// 目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
			LOG.info("Finished read record by Sql: [{}\n] {}.", querySql, basicMsg);
		} catch (Exception e) {
			throw RdbmsException.asQueryException(OBUtils.DATABASE_TYPE, e, querySql, table, username);
		} finally {
			DBUtil.closeDBResources(rs, stmt, conn);
		}
	}

}
