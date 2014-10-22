package com.alibaba.datax.plugin.reader.sqlserverreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RangeSplitUtil;
import com.alibaba.datax.plugin.reader.sqlserverreader.Constant;
import com.alibaba.datax.plugin.reader.sqlserverreader.Key;
import com.alibaba.datax.plugin.reader.sqlserverreader.SqlServerReaderErrorCode;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

@Deprecated
public class TableSplitUtil {
	private static final Logger LOG = LoggerFactory
			.getLogger(TableSplitUtil.class);

	private TableSplitUtil() {
	}

	// 任务切分
	public static List<Configuration> doSplit(Configuration readerSliceConfig,
			int adviceNumber) {
		List<Configuration> splittedConfigs = new ArrayList<Configuration>();

		boolean isTableMode = readerSliceConfig.getBool(Constant.IS_TABLE_MODE);

		int eachTableShouldSplittedNumber = -1;
		if (isTableMode) {
			int tableCount = readerSliceConfig.getInt(Constant.TABLE_NUMBER_MARK);
			// warn: check this
			eachTableShouldSplittedNumber = (int) Math.ceil(1.0 * adviceNumber
					/ tableCount);
		}

		String column = readerSliceConfig.getString(Key.COLUMN);
		String where = readerSliceConfig.getString(Key.WHERE);

		List<Object> conns = readerSliceConfig.getList(Constant.CONN_MARK,
				Object.class);
		String jdbcUrl;

		for (int i = 0; i < conns.size(); i++) {
			Configuration sliceConfig = readerSliceConfig.clone();

			Configuration connConf = Configuration
					.from(conns.get(i).toString());

			jdbcUrl = connConf.getString(Key.JDBC_URL);
			sliceConfig.set(Key.JDBC_URL, jdbcUrl);

			sliceConfig.remove(Constant.CONN_MARK);

			Configuration tempSlice;
			if (isTableMode) {
				List<String> tables = connConf.getList(Key.TABLE, String.class);

				if (null == tables || tables.isEmpty()) {
					String bussinessMessage = "source table configed error.";
					String message = StrUtil.buildOriginalCauseMessage(
							bussinessMessage, null);
					LOG.error(message);

					throw new DataXException(
							SqlServerReaderErrorCode.ILLEGAL_VALUE,
							bussinessMessage);
				}

				String splitPK = sliceConfig.getString(Key.SPLIT_PK);
				boolean needSplitTable = eachTableShouldSplittedNumber > 1
						&& StringUtils.isNotBlank(splitPK);

				if (needSplitTable) {
					for (String table : tables) {
						tempSlice = sliceConfig.clone();
						tempSlice.set(Key.TABLE, table);
						// warn : this should be eachTableShouldSplittedNumber
						List<Configuration> splittedSlices = TableSplitUtil
								.splitSingleTable(tempSlice,
										eachTableShouldSplittedNumber);

						for (Configuration splittedSlice : splittedSlices) {
							splittedConfigs.add(splittedSlice);
						}
					}
				} else {
					for (String table : tables) {
						tempSlice = sliceConfig.clone();
						tempSlice.set(Key.QUERY_SQL, TableSplitUtil
								.buildQuerySql(column, table, where));
						splittedConfigs.add(tempSlice);
					}
				}

			} else {
				// querySql mode
				List<String> sqls = connConf.getList(Key.QUERY_SQL,
						String.class);

				// TODO more than one querySql
				for (String querySql : sqls) {
					tempSlice = sliceConfig.clone();
					tempSlice.set(Key.QUERY_SQL, querySql);
					splittedConfigs.add(tempSlice);
				}
			}
		}
		return splittedConfigs;
	}

	public static List<Configuration> splitSingleTable(Configuration configuration,
			int adviceNum) {

		List<Configuration> pluginParams = new ArrayList<Configuration>();

		Pair<Object, Object> minMaxPK = getPKRange(configuration);

		if (null == minMaxPK) {
			throw new DataXException(SqlServerReaderErrorCode.ILLEGAL_SPLIT_PK,
					"split table with splitPk failed");
		}

		String splitPkName = configuration.getString(Key.SPLIT_PK);
		String column = configuration.getString(Key.COLUMN);
		String table = configuration.getString(Key.TABLE);
		String where = configuration.getString(Key.WHERE, null);
		boolean hasWhere = StringUtils.isNotBlank(where);

		configuration.set(Key.QUERY_SQL, buildQuerySql(column, table, where));

		if (null == minMaxPK.getLeft() || null == minMaxPK.getRight()) {
			pluginParams.add(configuration);
			return pluginParams;
		}

		boolean isStringType = false;
		if (minMaxPK.getLeft() instanceof String) {
			isStringType = true;
		}

		// warn : no need to see if minMaxPK.length is 2 now
		List<String> rangeList = null;
		if (isStringType) {
			rangeList = RangeSplitUtil.splitAndWrap(
					String.valueOf(minMaxPK.getLeft()),
					String.valueOf(minMaxPK.getRight()), adviceNum,
					splitPkName, "'", DataBaseType.SQLServer);
		} else {
			rangeList = RangeSplitUtil.splitAndWrap(
					Long.parseLong(minMaxPK.getLeft().toString()),
					Long.parseLong(minMaxPK.getRight().toString()), adviceNum,
					splitPkName);
		}

		String tempQuerySql = null;
		if (null != rangeList) {
			for (String range : rangeList) {
				Configuration conf = configuration.clone();

				tempQuerySql = buildQuerySql(column, table, where)
						+ (hasWhere ? " and " : " where ") + range;
				conf.set(Key.QUERY_SQL, tempQuerySql);
				pluginParams.add(conf);

				LOG.info("splitted tempQuerySql:" + tempQuerySql);
			}
		} else {
			pluginParams.add(configuration);
		}
		
		//deal pk is null
        Configuration tempConfig = configuration.clone();
        tempQuerySql = buildQuerySql(column, table, where)
                + (hasWhere ? " and " : " where ") + String.format(" %s IS NULL", splitPkName);

        LOG.info("After split, tempQuerySql=[\n{}\n].", tempQuerySql);

        tempConfig.set(Key.QUERY_SQL, tempQuerySql);
        pluginParams.add(tempConfig);

		return pluginParams;
	}

	protected static String buildQuerySql(String column, String table,
			String where) {
		String querySql = null;

		if (StringUtils.isBlank(where)) {
			querySql = String.format(Constant.QUERY_SQL_TEMPLATE_WHITOUT_WHERE,
					column, table);
		} else {
			querySql = String.format(Constant.QUERY_SQL_TEMPLATE, column,
					table, where);
		}

		return querySql;
	}

	private static Pair<Object, Object> getPKRange(Configuration plugin) {
		String pkRangeSQL = genPKRangeSQL(plugin);

		String jdbcURL = plugin.getString(Key.JDBC_URL);
		String username = plugin.getString(Key.USERNAME);
		String password = plugin.getString(Key.PASSWORD);
		int fetchSize = plugin.getInt(Key.FETCH_SIZE,
				Constant.DEFAULT_FETCH_SIZE);

		Connection conn = null;
		ResultSet rs = null;
		Pair<Object, Object> minMaxPK = null;
		try {
			conn = DBUtil.getConnection(DataBaseType.SQLServer, jdbcURL,
					username, password);
			rs = DBUtil.query(conn, pkRangeSQL, fetchSize);
			ResultSetMetaData rsMetaData = rs.getMetaData();
			if (isPKTypeValid(rsMetaData)) {
				while (rs.next()) {
					minMaxPK = new ImmutablePair<Object, Object>(
							rs.getObject(1), rs.getObject(2));
				}
			} else {
				// LOG.warn("pk type not long or string. split single table failed, use no-split strategy.");
				throw new DataXException(
						SqlServerReaderErrorCode.ILLEGAL_SPLIT_PK,
						"unsupported splitPk type，pk type not long nor string");
			}
		} catch (Exception e) {
			throw new DataXException(SqlServerReaderErrorCode.ILLEGAL_SPLIT_PK,
					"unsupported splitPk type，pk type not long nor string");
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

			boolean isNumberType = minType == Types.BIGINT
					|| minType == Types.INTEGER || minType == Types.SMALLINT
					|| minType == Types.TINYINT;

			boolean isStringType = minType == Types.CHAR
					|| minType == Types.NCHAR || minType == Types.VARCHAR
					|| minType == Types.LONGVARCHAR
					|| minType == Types.NVARCHAR;

			if (minType == maxType && (isNumberType || isStringType)) {
				ret = true;
			} else {
				throw new DataXException(
						SqlServerReaderErrorCode.ILLEGAL_SPLIT_PK,
						"unsupported splitPk type，pk type not long nor string");
			}
		} catch (SQLException e) {
			throw new DataXException(SqlServerReaderErrorCode.ILLEGAL_SPLIT_PK,
					"unsupported splitPk type，pk type not long nor string");
		}
		return ret;
	}

	private static String genPKRangeSQL(Configuration configuration) {

		String splitPK = configuration.getString(Key.SPLIT_PK).trim();
		String table = configuration.getString(Key.TABLE).trim();
		String where = configuration.getString(Key.WHERE, null);

		String minMaxTemplate = "SELECT MIN(%s),MAX(%s) FROM %s";
		String pkRangeSQL = String.format(minMaxTemplate, splitPK, splitPK,
				table);
		if (StringUtils.isNotBlank(where)) {
			pkRangeSQL = String.format("%s WHERE (%s AND %s IS NOT NULL)",
					pkRangeSQL, where, splitPK);
		}

		return pkRangeSQL;
	}

}