package com.alibaba.datax.plugin.writer.oceanbasev10writer.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.OceanBaseV10Writer;
import com.mysql.jdbc.Driver;

public class OBUtils {
	protected static final Logger LOG = LoggerFactory.getLogger(OceanBaseV10Writer.class);

	private static String CHECK_MEMSTORE = "select 1 from oceanbase.gv$memstore t where t.total>t.`limit` * ? limit 1";

	public static boolean isMemstoreFull(Connection conn, double memstoreThreshold) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(CHECK_MEMSTORE);
			ps.setDouble(1, memstoreThreshold);
			rs = ps.executeQuery();
			// 只要有满足条件的,则表示当前租户存在即将满的机器
			return rs.next();
		} catch (Throwable e) {
			LOG.error("check memstore fail", e);
			return false;
		} finally {
			DBUtil.closeDBResources(rs, ps, null);
		}
	}

	/**
	 * 
	 * @param tableName
	 * @param columns
	 * @param conn
	 * @param writeMode
	 * @return
	 */
	public static String buildWriteSql(String tableName, List<String> columnHolders, Connection conn, String writeMode) {
		List<String> valueHolders = new ArrayList<String>(columnHolders.size());
		for (int i = 0; i < columnHolders.size(); i++) {
			valueHolders.add("?");
		}
		boolean isWriteModeLegal = writeMode.trim().toLowerCase().startsWith("insert")
				|| writeMode.trim().toLowerCase().startsWith("replace");

		if (!isWriteModeLegal) {
			throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
					String.format("您所配置的 writeMode:%s 错误. 因为DataX 目前仅支持replace 或 insert 方式. 请检查您的配置并作出修改.", writeMode));
		}

		String writeDataSqlTemplate;
		if (writeMode.trim().toLowerCase().startsWith("replace")) {
			// upper case
			Set<String> skipColumns = getSkipColumns(conn, tableName);
			writeDataSqlTemplate = new StringBuilder().append("INSERT INTO " + tableName + " (")
					.append(StringUtils.join(columnHolders, ",")).append(") VALUES(")
					.append(StringUtils.join(valueHolders, ",")).append(")")
					.append(onDuplicateKeyUpdateString(columnHolders, skipColumns)).toString();
		} else {

			writeDataSqlTemplate = new StringBuilder().append(writeMode).append(" INTO %s (")
					.append(StringUtils.join(columnHolders, ",")).append(") VALUES(")
					.append(StringUtils.join(valueHolders, ",")).append(")").toString();
		}

		return writeDataSqlTemplate;
	}

	private static Set<String> getSkipColumns(Connection conn, String tableName) {
		String show = "show index from " + tableName;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(show);
			rs = ps.executeQuery();
			Map<String, Set<String>> uniqueKeys = new HashMap<String, Set<String>>();
			while (rs.next()) {
				String nonUnique = rs.getString("Non_unique");
				if (!"0".equals(nonUnique)) {
					continue;
				}
				String keyName = rs.getString("Key_name");
				String columnName = StringUtils.upperCase(rs.getString("Column_name"));
				Set<String> s = uniqueKeys.get(keyName);
				if (s == null) {
					s = new HashSet<String>();
					uniqueKeys.put(keyName, s);
				}
				s.add(columnName);
			}
			if (uniqueKeys.size() == 1) {
				return uniqueKeys.values().iterator().next();
			}
		} catch (Throwable e) {
			LOG.error("show index from table fail", e);
		} finally {
			DBUtil.closeDBResources(rs, ps, null);
		}
		return Collections.emptySet();
	}

	private static String onDuplicateKeyUpdateString(List<String> columnHolders, Set<String> skipColumns) {
		if (columnHolders == null || columnHolders.size() < 1) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		sb.append(" ON DUPLICATE KEY UPDATE ");
		List<String> list = new ArrayList<String>();
		for (String column : columnHolders) {
			// skip update columns
			if (skipColumns.contains(column.toUpperCase())) {
				continue;
			}
			list.add(column + "=VALUES(" + column + ")");
		}
		sb.append(StringUtils.join(list, ','));
		return sb.toString();
	}

	/**
	 * 休眠n秒
	 * 
	 * @param n
	 *            n秒
	 */
	public static void sleep(long n) {
		try {
			Thread.sleep(1000 * n);
		} catch (InterruptedException e) {
		}
	}
	
	public static void main(String[] args) throws SQLException {
		String url="jdbc:mysql://100.81.140.77:2881/snow";
		String user="root@sys";
		DriverManager.registerDriver(new Driver());
		Connection conn = DriverManager.getConnection(url, user, "");
		System.out.println(conn);
		String tableName="t";
		List<String> columnHolders = new ArrayList<String>();
		columnHolders.add("aaa");
		columnHolders.add("b");
		String sql = buildWriteSql(tableName, columnHolders, conn, "replace");
		System.out.println(sql);
	}
}
