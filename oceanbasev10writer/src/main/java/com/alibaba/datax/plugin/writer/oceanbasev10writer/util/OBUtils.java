package com.alibaba.datax.plugin.writer.oceanbasev10writer.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter.Task;

public class OBUtils {
	protected static final Logger LOG = LoggerFactory.getLogger(Task.class);

	private static String CHECK_MEMSTORE = "select 1 from oceanbase.gv$memstore t where t.total>t.mem_limit * ? limit 1";

	public static boolean isMemstoreFull(Connection conn, double memstoreThreshold) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean result = false;
		try {
			ps = conn.prepareStatement(CHECK_MEMSTORE);
			ps.setDouble(1, memstoreThreshold);
			rs = ps.executeQuery();
			// 只要有满足条件的,则表示当前租户 有个机器的memstore即将满
			result = rs.next();
		} catch (Throwable e) {
			LOG.error("check memstore fail"+e.getMessage());
			result = false;
		} finally {
			DBUtil.closeDBResources(rs, ps, null);
		}
		return result;
	}

	/**
	 * 
	 * @param tableName
	 * @param columns
	 * @param conn
	 * @param writeMode
	 * @return
	 */
	public static String buildWriteSql(String tableName, List<String> columnHolders, Connection conn) {
		List<String> valueHolders = new ArrayList<String>(columnHolders.size());
		for (int i = 0; i < columnHolders.size(); i++) {
			valueHolders.add("?");
		}
		// 一定是insert into on duplicate key update 模式
		String writeDataSqlTemplate;
		Set<String> skipColumns = getSkipColumns(conn, tableName);
		//TODO table  column  key word
		writeDataSqlTemplate = new StringBuilder().append("INSERT INTO " + tableName + " (")
				.append(StringUtils.join(columnHolders, ",")).append(") VALUES(")
				.append(StringUtils.join(valueHolders, ",")).append(")")
				.append(onDuplicateKeyUpdateString(columnHolders, skipColumns)).toString();
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
			LOG.error("show index from table fail :" + show, e);
		} finally {
			DBUtil.closeDBResources(rs, ps, null);
		}
		return Collections.emptySet();
	}

	private static String onDuplicateKeyUpdateString(List<String> columnHolders, Set<String> skipColumns) {
		if (columnHolders == null || columnHolders.size() < 1) {
			return "";
		}
		StringBuilder builder = new StringBuilder();
		builder.append(" ON DUPLICATE KEY UPDATE ");
		List<String> list = new ArrayList<String>();
		for (String column : columnHolders) {
			// skip update columns
			if (skipColumns.contains(column.toUpperCase())) {
				continue;
			}
			list.add(column + "=VALUES(" + column + ")");
		}
		if (!list.isEmpty()) {
			builder.append(StringUtils.join(list, ','));
		} else {
			// 如果除了UK 没有别的字段,则更新第一个字段
			String column = columnHolders.get(0);
			builder.append(column + "=VALUES(" + column + ")");
		}
		return builder.toString();
	}

	/**
	 * 休眠n毫秒
	 * 
	 * @param ms
	 *            毫秒
	 */
	public static void sleep(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
		}
	}
}
