package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

import java.sql.Connection;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

public class OBUtils {

	private static final Logger LOG = LoggerFactory.getLogger(OBUtils.class);

	public static final DataBaseType DATABASE_TYPE = DataBaseType.MySql;

	public static void initConn4Reader(Connection conn, long queryTimeoutSeconds) {
		String setQueryTimeout = "set ob_query_timeout=" + (queryTimeoutSeconds * 1000 * 1000L);
		String setTrxTimeout = "set ob_trx_timeout=" + ((queryTimeoutSeconds + 5) * 1000 * 1000L);
		Statement stmt = null;
		try {
			conn.setAutoCommit(true);
			LOG.warn("setAutoCommit=true");
			stmt = conn.createStatement();
			stmt.execute(setQueryTimeout);
			LOG.warn(setQueryTimeout);
			stmt.execute(setTrxTimeout);
			LOG.warn(setTrxTimeout);
		} catch (Throwable e) {
			LOG.warn("initConn4Reader fail", e);
		} finally {
			DBUtil.closeDBResources(stmt, null);
		}
	}
}
