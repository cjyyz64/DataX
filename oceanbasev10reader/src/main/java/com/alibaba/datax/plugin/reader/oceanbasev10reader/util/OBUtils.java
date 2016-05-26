package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
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

	public static void sleep(int ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
		}
	}

	/**
	 * 
	 * @param conn
	 * @param table
	 * @param columns
	 * @param context
	 */
	public static void matchPkIndexs(Connection conn, String table, List<String> columns, TaskContext context) {
		String[] pkColumns = getPkColumns(conn, table);
		if (ArrayUtils.isEmpty(pkColumns)) {
			LOG.warn("table=" + table + " has no primary key");
			return;
		}
		context.setPkColumns(pkColumns);
		int[] pkIndexs = new int[pkColumns.length];
		for (int i = 0, n = pkColumns.length; i < n; i++) {
			String pkc = pkColumns[i];
			int j = 0;
			for (int k = columns.size(); j < k; j++) {
				if (StringUtils.equalsIgnoreCase(pkc, columns.get(j))) {
					pkIndexs[i] = j;
					break;
				}
			}
			// 到这里 说明主键列不在columns中,则主动追加到尾部
			if (j == columns.size()) {
				columns.add(pkc);
				pkIndexs[i] = columns.size();
			}
		}
		context.setPkIndexs(pkIndexs);
	}

	private static String[] getPkColumns(Connection conn, String table) {
		String show = "show index from " + table + " where Key_name='PRIMARY'";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(show);
			rs = ps.executeQuery();
			Map<Integer, String> pkColumns = new HashMap<Integer, String>();
			while (rs.next()) {
				int seq = rs.getInt("Seq_in_index");
				String columnName = StringUtils.lowerCase(rs.getString("Column_name"));
				pkColumns.put(seq, columnName);
			}
			String[] pks = new String[pkColumns.size()];
			for (Entry<Integer, String> entry : pkColumns.entrySet()) {
				pks[entry.getKey() - 1] = entry.getValue();
			}
			return pks;
		} catch (Throwable e) {
			LOG.error("show index from table fail :" + show, e);
		} finally {
			DBUtil.closeDBResources(rs, ps, null);
		}
		return null;
	}

	/**
	 * 首次查的SQL
	 * 
	 * @param context
	 * @return
	 */
	public static String buildFirstQuerySql(TaskContext context) {
		String sql = "select " + StringUtils.join(context.getColumns(), ',');
		sql += " from " + context.getTable() + " ";
		if (StringUtils.isNotEmpty(context.getWhere())) {
			sql += " where " + context.getWhere();
		}
		sql += " order by " + StringUtils.join(context.getPkColumns(), ',') + " asc";
		return sql;
	}

	/**
	 * 增量查的SQL
	 * 
	 * @param column
	 * @param table
	 * @param where
	 * @param pkColumns
	 * @return
	 */
	public static String buildAppendQuerySql(TaskContext context) {
		Character[] placeHolders = new Character[context.getPkColumns().length];
		for (int i = 0; i < context.getPkColumns().length; i++) {
			placeHolders[i] = '?';
		}
		String append = "(" + StringUtils.join(context.getPkColumns(), ',') + ") > ("
				+ StringUtils.join(placeHolders, ',') + ")";
		String sql = "select " + StringUtils.join(context.getColumns(), ',') + " from " + context.getTable()
				+ " where ";
		if (StringUtils.isNotEmpty(context.getWhere())) {
			sql += context.getWhere() + " and " + append;
		} else {
			sql += append;
		}
		sql += " order by " + StringUtils.join(context.getPkColumns(), ',') + " asc";
		return sql;
	}

	/**
	 * 由于ObProxy存在bug,事务超时或事务被杀时,conn的close是没有响应的
	 * @param rs
	 * @param stmt
	 * @param conn
	 */
	public static void asyncClose(final ResultSet rs, final Statement stmt, final Connection conn) {
		Thread t = new Thread() {
			public void run() {
				DBUtil.closeDBResources(rs, stmt, conn);
			}
		};
		t.setDaemon(true);
		t.start();
	}
}
