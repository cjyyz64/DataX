package com.alibaba.datax.plugin.reader.sqlserverreader.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.reader.sqlserverreader.Constants;
import com.alibaba.datax.plugin.reader.sqlserverreader.Key;
import com.alibaba.datax.plugin.reader.sqlserverreader.SqlServerReaderErrorCode;

public class TableSplitUtil {
	private static final Logger LOG = LoggerFactory
			.getLogger(TableSplitUtil.class);

	private TableSplitUtil() {
	}
	
	// 任务切分
		public static List<Configuration> doSplit(Configuration originReaderConfig,
				int adviceNumber) {
			List<Configuration> splittedConfigs = new ArrayList<Configuration>();

			boolean isTableMode = originReaderConfig.getBool(Constants.TABLE_MODE);

			int eachTableShouldSplittedNumber = -1;
			if (isTableMode) {
				int tableCount = originReaderConfig.getInt(Constants.TABLE_NUMBER);
				// TODO check this
				eachTableShouldSplittedNumber = tableCount / adviceNumber;
			}

			String column = originReaderConfig.getString(Key.COLUMN);
			String where = originReaderConfig.getString(Key.WHERE);

			List<Object> conns = originReaderConfig.getList(Constants.CONNECTION,
					Object.class);
			String jdbcUrl;

			for (int i = 0; i < conns.size(); i++) {
				Configuration sliceConfig = originReaderConfig.clone();

				Configuration connConf = Configuration
						.from(conns.get(i).toString());

				jdbcUrl = connConf.getString(Key.JDBC_URL);
				sliceConfig.set(Key.JDBC_URL, jdbcUrl);

				sliceConfig.remove(Constants.CONNECTION);

				Configuration tempSlice;
				if (isTableMode) {
					List<String> tables = connConf.getList(Key.TABLE, String.class);

					if (null == tables || tables.isEmpty()) {
						throw new DataXException(
								SqlServerReaderErrorCode.CONF_ERROR,
								"source table configed error");
					}

					String splitPK = sliceConfig.getString(Key.SPLIT_PK);
					boolean needSplitTable = eachTableShouldSplittedNumber > 1
							&& StringUtils.isNotBlank(splitPK);

					if (needSplitTable) {
						for (String table : tables) {
							tempSlice = sliceConfig.clone();
							tempSlice.set(Key.TABLE, table);
//							List<Configuration> splittedSlices = TableSplitUtil
//									.splitSingleTable(tempSlice,
//											eachTableShouldSplittedNumber);
							//warn : this should be adviceNumber
							List<Configuration> splittedSlices = TableSplitUtil
									.splitSingleTable(tempSlice,
											adviceNumber);

							for (Configuration splittedSlice : splittedSlices) {
								splittedConfigs.add(splittedSlice);
							}
						}
					} else {
						for (String table : tables) {
							tempSlice = sliceConfig.clone();
							tempSlice.set(Key.QUERYSQL, TableSplitUtil
									.buildQuerySql(column, table, where));
							splittedConfigs.add(tempSlice);
						}
					}

				} else {
					// querySql mode
					List<String> sqls = connConf
							.getList(Key.QUERYSQL, String.class);

					// TODO more than one querySql
					for (String querySql : sqls) {
						tempSlice = sliceConfig.clone();
						tempSlice.set(Key.QUERYSQL, querySql);
						splittedConfigs.add(tempSlice);
					}
				}
			}
			return splittedConfigs;
		}

	public static List<Configuration> splitSingleTable(Configuration plugin,
			int adviceNum) {
		List<Long> minMaxPK = getPKRange(plugin);
		List<Configuration> pluginParams = appendRangeSQL(plugin, minMaxPK,
				adviceNum);

		return pluginParams;
	}

	private static List<Configuration> appendRangeSQL(
			Configuration pluginParam, List<Long> minMaxPK, int splitNumber) {
		List<Configuration> pluginParams = new ArrayList<Configuration>();
		if (2 == minMaxPK.size()) {
			long min = minMaxPK.get(0);
			long max = minMaxPK.get(1);

			long step = Math.round((max - min + 1.0) / splitNumber);
			String column = pluginParam.getString(Key.COLUMN);
			String table = pluginParam.getString(Key.TABLE);
			String splitPK = pluginParam.getString(Key.SPLIT_PK);
			String where = pluginParam.getString(Key.WHERE, null);

			boolean hasWhere = StringUtils.isNotBlank(where);
			//different rds is different
			String template = "[%s] >= %s and [%s] < %s";

			long lastIndex = min;
			long currentIndex = min;
			String tempSQL = null;
			Configuration tempPlugin = null;
			for (int i = 1; i < splitNumber; i++) {
				currentIndex = lastIndex + step;
				tempSQL = buildQuerySql(column, table, where)
						+ (hasWhere ? " and " : " where ")
						+ String.format(template, splitPK, lastIndex, splitPK,
								currentIndex);
				tempPlugin = pluginParam.clone();
				tempPlugin.set(Key.QUERYSQL, tempSQL);
				pluginParams.add(tempPlugin);
				lastIndex = currentIndex;
			}
			// 处理lastIndex到max区间的值
			String specialTemplate = "[%s] >= %s and [%s] <= %s";
			tempSQL = buildQuerySql(column, table, where)
					+ (hasWhere ? " and " : " where ")
					+ String.format(specialTemplate, splitPK, lastIndex,
							splitPK, max);
			tempPlugin = pluginParam.clone();
			tempPlugin.set(Key.QUERYSQL, tempSQL);
			pluginParams.add(tempPlugin);
		} else {
			pluginParams.add(pluginParam);
		}
		return pluginParams;
	}

	protected static String buildQuerySql(String column, String table,
			String where) {
		String querySql = null;

		if (StringUtils.isBlank(where)) {
			querySql = String.format(Constants.QUERY_SQL_TEMPLATE_WHITOUT_WHERE,
					column, table);
		} else {
			querySql = String.format(Constants.QUERY_SQL_TEMPLATE, column,
					table, where);
		}

		return querySql;
	}

	@SuppressWarnings("resource")
	private static List<Long> getPKRange(Configuration plugin) {
		List<String> sqls = genPKRangeSQL(plugin);

		String checkPKSQL = sqls.get(0);
		String pkRangeSQL = sqls.get(1);

		String jdbcURL = plugin.getString(Key.JDBC_URL);
		String username = plugin.getString(Key.USERNAME);
		String password = plugin.getString(Key.PASSWORD);
		
		int fetchSize = plugin.getInt(Key.FETCH_SIZE, 32);
		
		Connection conn = null;
		ResultSet rs = null;
		List<Long> minMaxPK = new ArrayList<Long>();
		try {
			conn = DBUtil.getConnection("sqlserver", jdbcURL, username, password);
			rs = DBUtil.query(conn, checkPKSQL, fetchSize);
			while (rs.next()) {
				if (rs.getLong(1) > 0L) {
					throw new DataXException(SqlServerReaderErrorCode.CONF_ERROR,
							"Configed PK has null value!");
				}
			}
			rs = DBUtil.query(conn, pkRangeSQL, fetchSize);
			ResultSetMetaData rsMetaData = rs.getMetaData();
			if (isPKTypeValid(rsMetaData)) {
				while (rs.next()) {
					minMaxPK.add(rs.getLong(1));
					minMaxPK.add(rs.getLong(2));
				}
			} else {
				LOG.warn("pk type not INTEGER nor BIGINT. split single table failed, use no-split strategy.");
			}
		} catch (Exception e) {
			if (LOG.isDebugEnabled()) {
				LOG.error(e.getMessage(), e);
			}
			LOG.warn("split single table failed, use no-split strategy.");
		} finally {
			DBUtil.closeDBResources(rs, null, conn);
		}

		return minMaxPK;
	}

	private static boolean isPKTypeValid(ResultSetMetaData rsMetaData) {
		boolean ret = false;
		try {
			int minType = rsMetaData.getColumnType(1);
			int maxType = rsMetaData.getColumnType(2);
			if (minType == maxType
					&& (minType == Types.BIGINT || minType == Types.INTEGER
							|| minType == Types.SMALLINT || minType == Types.TINYINT)) {
				ret = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	// TOOD where 条件上添加()
	private static List<String> genPKRangeSQL(Configuration plugin) {
		List<String> sqls = new ArrayList<String>();

		String splitPK = plugin.getString(Key.SPLIT_PK).trim();
		String table = plugin.getString(Key.TABLE).trim();
		String where = plugin.getString(Key.WHERE, null);

		String checkPKTemplate = "SELECT COUNT(1) FROM %s WHERE [%s] IS NULL";
		String checkPKSQL = String.format(checkPKTemplate, table, splitPK);

		String minMaxTemplate = "SELECT MIN([%s]),MAX([%s]) FROM %s";
		String pkRangeSQL = String.format(minMaxTemplate, splitPK, splitPK,
				table);
		if (StringUtils.isNotBlank(where)) {
			pkRangeSQL += " WHERE " + where;
		}

		sqls.add(checkPKSQL);
		sqls.add(pkRangeSQL);
		return sqls;
	}

}