package com.alibaba.datax.plugin.writer.oceanbasev10writer.task;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.Config;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.OBUtils;

public class SingleTableWriterTask extends CommonRdbmsWriter.Task {

	// memstore_total 与 memstore_limit 比例的阈值,一旦超过这个值,则暂停写入
	private double memstoreThreshold = Config.DEFAULT_MEMSTORE_THRESHOLD;

	// memstore检查的间隔
	private long memstoreCheckIntervalSecond = Config.DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND;

	// 最后一次检查
	private long lastCheckMemstoreTime;

	// 失败重试次数 经与强思讨论 暂时不做
	// 未来需明确哪些OceanBase异常 可重试的再做
	// private int failTryCount = Config.DEFAULT_FAIL_TRY_COUNT;

	public SingleTableWriterTask(DataBaseType dataBaseType) {
		super(dataBaseType);
	}

	@Override
	public void init(Configuration config) {
		super.init(config);
		this.memstoreThreshold = config.getDouble(Config.MEMSTORE_THRESHOLD, Config.DEFAULT_MEMSTORE_THRESHOLD);
		this.memstoreCheckIntervalSecond = config.getLong(Config.MEMSTORE_CHECK_INTERVAL_SECOND,
				Config.DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND);
		// failTryCount = config.getInt(Config.FAIL_TRY_COUNT,
		// Config.DEFAULT_FAIL_TRY_COUNT);
		// OceanBase 所有操作都是 insert into on duplicate key update 模式
		// TODO writeMode应该使用enum来定义
		this.writeMode = "update";
		rewriteSql();
	}

	private void rewriteSql() {
		Connection conn = DBUtil.getConnection(this.dataBaseType, this.jdbcUrl, username, password);
		try {
			this.writeRecordSql = OBUtils.buildWriteSql(table, columns, conn);
		} finally {
			DBUtil.closeDBResources(null, null, conn);
		}
	}

	protected void doBatchInsert(Connection connection, List<Record> buffer) throws SQLException {
		// 检查内存
		checkMemstore(connection);
		super.doBatchInsert(connection, buffer);
	}

	private void checkMemstore(Connection conn) {
		long now = System.currentTimeMillis();
		if (now - lastCheckMemstoreTime < 1000 * memstoreCheckIntervalSecond) {
			return;
		}
		while (OBUtils.isMemstoreFull(conn, memstoreThreshold)) {
			LOG.warn("OB memstore is full,sleep 60 seconds, threshold=" + memstoreThreshold);
			OBUtils.sleep(60000);
		}
		lastCheckMemstoreTime = now;
	}
}
