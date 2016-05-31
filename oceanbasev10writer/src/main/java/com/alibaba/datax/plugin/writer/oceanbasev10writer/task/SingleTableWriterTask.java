package com.alibaba.datax.plugin.writer.oceanbasev10writer.task;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.Config;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ConnHolder;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.ObWriterUtils;

public class SingleTableWriterTask extends CommonRdbmsWriter.Task {

	// memstore_total 与 memstore_limit 比例的阈值,一旦超过这个值,则暂停写入
	private double memstoreThreshold = Config.DEFAULT_MEMSTORE_THRESHOLD;

	// memstore检查的间隔
	private long memstoreCheckIntervalSecond = Config.DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND;

	// 最后一次检查
	private long lastCheckMemstoreTime;

	// 失败重试次数
	private int failTryCount = Config.DEFAULT_FAIL_TRY_COUNT;

	private ConnHolder connHolder;

	public SingleTableWriterTask(DataBaseType dataBaseType) {
		super(dataBaseType);
	}

	@Override
	public void init(Configuration config) {
		super.init(config);
		this.memstoreThreshold = config.getDouble(Config.MEMSTORE_THRESHOLD, Config.DEFAULT_MEMSTORE_THRESHOLD);
		this.memstoreCheckIntervalSecond = config.getLong(Config.MEMSTORE_CHECK_INTERVAL_SECOND,
				Config.DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND);
		failTryCount = config.getInt(Config.FAIL_TRY_COUNT, Config.DEFAULT_FAIL_TRY_COUNT);
		// OceanBase 所有操作都是 insert into on duplicate key update 模式
		// writeMode应该使用enum来定义
		this.writeMode = "update";
		this.connHolder = new ConnHolder(config, jdbcUrl, username, password);
		rewriteSql();
	}

	private void rewriteSql() {
		Connection conn = connHolder.initConnection();
		this.writeRecordSql = ObWriterUtils.buildWriteSql(table, columns, conn);
	}

	protected void doBatchInsert(Connection conn, List<Record> buffer) throws SQLException {
		doBatchInsert(buffer);
	}

	private void doBatchInsert(List<Record> buffer) {
		Connection conn = connHolder.getConn();
		// 检查内存
		checkMemstore(conn);
		boolean success = false;
		try {
			for (int i = 0; i < failTryCount; i++) {
				PreparedStatement ps = null;
				try {
					conn.setAutoCommit(false);
					ps = conn.prepareStatement(this.writeRecordSql);
					for (Record record : buffer) {
						ps = fillPreparedStatement(ps, record);
						ps.addBatch();
					}
					ps.executeBatch();
					conn.commit();
				} catch (SQLException e) {
					// 如果是OB系统级异常,则需要重建连接
					boolean fatalFail = ObWriterUtils.isFatalError(e);
					if (fatalFail) {
						LOG.warn(
								"遇到OB致命异常,回滚此次写入, 休眠 5分钟,SQLState:" + e.getSQLState() + ",ErrorCode:"
										+ e.getErrorCode(), e);
						ObWriterUtils.sleep(300000);
						conn = connHolder.reconnect();
						// 如果是可恢复的异常,则重试
					} else if (ObWriterUtils.isRecoverableError(e)) {
						LOG.warn(
								"遇到OB可恢复异常,回滚此次写入, 休眠 1分钟,SQLState:" + e.getSQLState() + ",ErrorCode:"
										+ e.getErrorCode(), e);
						conn.rollback();
						ObWriterUtils.sleep(60000);
						// 其它异常直接退出,采用逐条写入方式
					} else {
						LOG.warn(
								"遇到OB异常,回滚此次写入, 休眠 1秒,采用逐条写入提交,SQLState:" + e.getSQLState() + ",ErrorCode:"
										+ e.getErrorCode(), e);
						conn.rollback();
						ObWriterUtils.sleep(1000);
						break;
					}
				} finally {
					DBUtil.closeDBResources(ps, null);
				}
			}
		} catch (SQLException e) {
			LOG.warn("遇到OB异常,回滚此次写入, 采用逐条写入提交,SQLState:" + e.getSQLState() + ",ErrorCode:" + e.getErrorCode(), e);
		}
		if (!success) {
			doOneInsert(conn, buffer);
		}
	}

	private void checkMemstore(Connection conn) {
		long now = System.currentTimeMillis();
		if (now - lastCheckMemstoreTime < 1000 * memstoreCheckIntervalSecond) {
			return;
		}
		while (ObWriterUtils.isMemstoreFull(conn, memstoreThreshold)) {
			LOG.warn("OB memstore is full,sleep 60 seconds, threshold=" + memstoreThreshold);
			ObWriterUtils.sleep(60000);
		}
		lastCheckMemstoreTime = now;
	}

	@Override
	public void destroy(Configuration writerSliceConfig) {
		// 把本级持有的conn关闭掉
		DBUtil.closeDBResources(null, connHolder.getConn());
		super.destroy(writerSliceConfig);
	}
}
