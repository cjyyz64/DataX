package com.alibaba.datax.plugin.reader.oceanbasev10reader.ext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.Config;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.OBUtils;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.TaskContext;

public class ReaderTask extends CommonRdbmsReader.Task {
	private static final Logger LOG = LoggerFactory.getLogger(ReaderTask.class);
	private int taskGroupId = -1;
	private int taskId = -1;

	private String username;
	private String password;
	private String jdbcUrl;
	private String mandatoryEncoding;
	private int queryTimeoutSeconds;//查询超时 默认48小时
	private  boolean isOb=true;//考虑与MySQL的兼容性,这里标识出来用于

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

        //ob10的处理
        if (this.jdbcUrl.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING)) {
            String[] ss = this.jdbcUrl.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
            if (ss.length != 3) {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
            }
            LOG.info("this is ob1_0 jdbc url.");
            this.username = ss[1].trim() +":"+this.username;
            this.jdbcUrl = ss[2];
            LOG.info("this is ob1_0 jdbc url. user=" + this.username + " :url=" + this.jdbcUrl);
        }else{
        	isOb=false;
        }

        this.mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "");
	}

	/**
	 * 
	 * 如果isTableMode && table有PK 
	 * <p>
	 * 则支持断点续读  (若pk不在原始的columns中,则追加到尾部,但不传给下游)
	 * <p>
	 * 否则,则使用旧模式
	 */
	@Override
	public void startRead(Configuration readerSliceConfig, RecordSender recordSender,
			TaskPluginCollector taskPluginCollector, int fetchSize) {
		//如果不是OB则走MySQL的实现
		if(!isOb){
			super.startRead(readerSliceConfig, recordSender, taskPluginCollector, fetchSize);
			return;
		}
		String querySql = readerSliceConfig.getString(Key.QUERY_SQL);
		String table = readerSliceConfig.getString(Key.TABLE);
		PerfTrace.getInstance().addTaskDetails(taskId, table + "," + jdbcUrl);
		LOG.info("Begin to read record by Sql: [{}\n] {}.", querySql, jdbcUrl);
		List<String> columns = readerSliceConfig.getList(Key.COLUMN_LIST, String.class);
		String where = readerSliceConfig.getString(Key.WHERE);
		TaskContext context = new TaskContext(table, columns, where, fetchSize);
		context.setQuerySql(querySql);
		PerfRecord allPerf = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
		allPerf.start();
		boolean isTableMode = readerSliceConfig.getBool(Constant.IS_TABLE_MODE);
		startRead0(isTableMode, context, recordSender, taskPluginCollector);
		allPerf.end(context.getCost());
		// 目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
		LOG.info("Finished read record by Sql: [{}\n] {}.", context.getQuerySql(), jdbcUrl);
	}

	private void startRead0(boolean isTableMode, TaskContext context, RecordSender recordSender,
			TaskPluginCollector taskPluginCollector) {
		// 不是table模式 直接使用原来的做法
		if (!isTableMode) {
			doRead(recordSender, taskPluginCollector, context);
			return;
		}
		Connection conn = DBUtil.getConnection(OBUtils.DATABASE_TYPE, jdbcUrl, username, password);
		try {
		} catch (Throwable e) {
			OBUtils.matchPkIndexs(conn, context.getTable(), context.getColumns(), context);
			LOG.warn("fetch PkIndexs fail,table=" + context.getTable(), e);
		} finally {
			DBUtil.closeDBResources(null, null, conn);
		}
		// 如果不是table 且 pk不存在 或pk不在select中 则仍然使用原来的做法
		if (context.getPkIndexs() == null) {
			doRead(recordSender, taskPluginCollector, context);
			return;
		}

		String getFirstQuerySql = OBUtils.buildFirstQuerySql(context);
		// 从这里开始就是 断点续读功能
		// while(true) {
		// 正常读 (需 order by pk asc)
		// 如果遇到失败,分两种情况:
		// a)已读出记录,则开始走增量读逻辑
		// b)未读出记录,则走正常读逻辑(仍然需要order by pk asc)
		// 正常结束 则 break
		// }
		LOG.warn("start table primary key scan");
		context.setQuerySql(getFirstQuerySql);
		String appendQuerySql = OBUtils.buildAppendQuerySql(context);
		while (true) {
			try {
				doRead(recordSender, taskPluginCollector, context);
				break;
			} catch (Throwable e) {
				LOG.error("read fail,sleep 60 second,save point:" + context.getSavePoint(), e);
				OBUtils.sleep(60000);
				// 假如原来的查询有查出数据,则改成增量查询
				if (context.getPkIndexs() != null && context.getSavePoint() != null) {
					context.setQuerySql(appendQuerySql);
				}
			}
		}
	}

	private void doRead(RecordSender recordSender, TaskPluginCollector taskPluginCollector, TaskContext context) {
		Connection conn = DBUtil.getConnection(OBUtils.DATABASE_TYPE, jdbcUrl, username, password);
		// session config .etc related
		// DBUtil.dealWithSessionConfig(conn, readerSliceConfig,
		// OBUtils.DATABASE_TYPE, basicMsg);
		// OceanBase special session config
		OBUtils.initConn4Reader(conn, queryTimeoutSeconds);

		Statement stmt = null;
		ResultSet rs = null;
		PerfRecord perfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.SQL_QUERY);
		perfRecord.start();
		AsyncSendTask asyncSend = new AsyncSendTask(recordSender, context, 1024);
		asyncSend.start();
		try {
			LOG.warn("exe sql:" + context.getQuerySql());
			if (context.getPkIndexs() == null || context.getSavePoint() == null) {
				stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				stmt.setFetchSize(context.getFetchSize());
				stmt.setQueryTimeout(queryTimeoutSeconds);
				rs = stmt.executeQuery(context.getQuerySql());
			} else {
				PreparedStatement ps = conn.prepareStatement(context.getQuerySql(), ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY);
				stmt = ps;
				for (int i = 0, n = context.getPkIndexs().length; i < n; i++) {
					ps.setObject(i + 1, context.getSavePoint().getColumn(context.getPkIndexs()[i]).getRawData());
				}
				ps.setFetchSize(context.getFetchSize());
				ps.setQueryTimeout(queryTimeoutSeconds);
				rs = ps.executeQuery();
			}
			perfRecord.end();

			ResultSetMetaData metaData = rs.getMetaData();
			int columnNumber = metaData.getColumnCount();
			long lastTime = System.nanoTime();
			// int i = 0;
			while (rs.next()) {
				context.addCost(System.nanoTime() - lastTime);
				// 仅仅buildRecord没有真正send
				Record savePoint = buildRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding,
						taskPluginCollector);
				asyncSend.send(savePoint);
				lastTime = System.nanoTime();
				// mock 每1000条 停10ms
				// i++;
				// if (i >= 1000) {
				// OBUtils.sleep(10);
				// i = 0;
				// }
			}
		} catch (Exception e) {
			LOG.error("reader data fail", e);
			throw RdbmsException.asQueryException(OBUtils.DATABASE_TYPE, e, context.getQuerySql(), context.getTable(),
					username);
		} finally {
			asyncSend.shutdown();
			OBUtils.asyncClose(rs, stmt, conn);
			try {
				asyncSend.join();
			} catch (InterruptedException e) {
				//没有正常做完也不要紧,因为context的 SavePoint的根据 真正的send记录来保存的
			}
		}
	}

}
