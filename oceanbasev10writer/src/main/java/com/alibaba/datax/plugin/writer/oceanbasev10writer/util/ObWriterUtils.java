package com.alibaba.datax.plugin.writer.oceanbasev10writer.util;

import java.sql.Connection;
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

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter.Task;

public class ObWriterUtils {
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
			LOG.error("check memstore fail" + e.getMessage());
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
		// TODO table column key word
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

	/**
	 * 致命错误
	 * 
	 * @param e
	 * @return
	 */
	public static boolean isFatalError(SQLException e) {
		String sqlState = e.getSQLState();
		final int errorCode = Math.abs(e.getErrorCode());
		if (StringUtils.startsWith(sqlState, "08")) {
			return true;
		}
		switch (errorCode) {
		// Communications Errors
		case 1040: // ER_CON_COUNT_ERROR
		case 1042: // ER_BAD_HOST_ERROR
		case 1043: // ER_HANDSHAKE_ERROR
		case 1047: // ER_UNKNOWN_COM_ERROR
		case 1081: // ER_IPSOCK_ERROR
		case 1129: // ER_HOST_IS_BLOCKED
		case 1130: // ER_HOST_NOT_PRIVILEGED
			// Authentication Errors
		case 1045: // ER_ACCESS_DENIED_ERROR
			// Resource errors
		case 1004: // ER_CANT_CREATE_FILE
		case 1005: // ER_CANT_CREATE_TABLE
		case 1015: // ER_CANT_LOCK
		case 1021: // ER_DISK_FULL
		case 1041: // ER_OUT_OF_RESOURCES
		case 1094: // Unknown thread id: %lu
			// Out-of-memory errors
		case 1037: // ER_OUTOFMEMORY
		case 1038: // ER_OUT_OF_SORTMEMORY
			return true;
		}

		if (StringUtils.isNotBlank(e.getMessage())) {
			final String errorText = e.getMessage().toUpperCase();

			if (errorCode == 0
					&& (errorText.indexOf("COMMUNICATIONS LINK FAILURE") > -1 || errorText
							.indexOf("COULD NOT CREATE CONNECTION") > -1) || errorText.indexOf("NO DATASOURCE") > -1
					|| errorText.indexOf("NO ALIVE DATASOURCE") > -1
					|| errorText.indexOf("NO OPERATIONS ALLOWED AFTER CONNECTION CLOSED") > -1) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 可恢复的错误
	 * 
	 * @param e
	 * @return
	 */
	public static boolean isRecoverableError(SQLException e) {
		int error = Math.abs(e.getErrorCode());
		//明确可恢复
		if(white.contains(error)){
			return true;
		}
		//明确不可恢复
		if(black.contains(error)){
			return false;
		}
		//超过4000的,都是OB特有的ErrorCode
		return error > 4020;
	}
	
	private static Set<Integer> white = new HashSet<Integer>();
	static{
		int[] errList={1213,1047,1041,1094,4000,4012};
		for(int err:errList){
			white.add(err);
		}
	}
	//不考虑4000以下的
	private static Set<Integer> black = new HashSet<Integer>();
	static{
		int[] errList={4022,4025,4026,4028,4029,4031,4033,4034,4037,4041,4044};
		for(int err:errList){
			black.add(err);
		}
	}
	
}
