package com.alibaba.datax.plugin.reader.sqlserverreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.TableExpandUtil;
import com.alibaba.datax.plugin.reader.sqlserverreader.Constant;
import com.alibaba.datax.plugin.reader.sqlserverreader.Key;
import com.alibaba.datax.plugin.reader.sqlserverreader.SqlServerReaderErrorCode;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ConfigPretreatmentUtil {
	private static Logger LOG = LoggerFactory
			.getLogger(ConfigPretreatmentUtil.class);

	private ConfigPretreatmentUtil() {
	}

	/**
	 * 验证原始DataX sqlserverreader 配置合法性，预处理
	 * 
	 * @param originReaderConfig
	 *            original reader config
	 * */
	public static void doPretreatment(Configuration originReaderConfig) {
		originReaderConfig.getNecessaryValue(Key.USERNAME,
				SqlServerReaderErrorCode.REQUIRED_VALUE);
		originReaderConfig.getNecessaryValue(Key.PASSWORD,
				SqlServerReaderErrorCode.REQUIRED_VALUE);

		int fetchSize = originReaderConfig.getInt(Key.FETCH_SIZE,
				Constant.DEFAULT_FETCH_SIZE);
		if (fetchSize < 1) {
			throw new DataXException(SqlServerReaderErrorCode.REQUIRED_VALUE,
					"fetchSize can not less than 1.");
		}
		originReaderConfig.set(Key.FETCH_SIZE, fetchSize);

		// connect part
		List<Object> conns = originReaderConfig.getList(Constant.CONN_MARK,
				Object.class);

		// warn:to sql server ,connect part array size must be 1
		if (null == conns || 1 != conns.size()) {
			throw new DataXException(SqlServerReaderErrorCode.ILLEGAL_VALUE,
					"connection configuration part invalid, shall have one and only one element");
		}

		boolean isTableMode = recognizeTableOrQuerySqlMode(originReaderConfig);
		originReaderConfig.set(Constant.IS_TABLE_MODE, isTableMode);

		dealJdbcAndTableConfig(originReaderConfig);

		dealColumnConfig(originReaderConfig);
	}

	// resolve table mode or querySql mode
	private static boolean recognizeTableOrQuerySqlMode(
			Configuration originReaderConfig) {

		List<Object> conns = originReaderConfig.getList(Constant.CONN_MARK,
				Object.class);

		List<Boolean> tableModeFlags = new ArrayList<Boolean>();
		List<Boolean> querySqlModeFlags = new ArrayList<Boolean>();

		String table;
		String querySql;

		boolean isTableMode = false;
		boolean isQuerySqlMode = false;
		for (int i = 0; i < conns.size(); i++) {
			Configuration connConf = Configuration
					.from(conns.get(i).toString());

			table = connConf.getString(Key.TABLE);
			querySql = connConf.getString(Key.QUERY_SQL);

			isTableMode = StringUtils.isNotBlank(table);
			tableModeFlags.add(isTableMode);

			isQuerySqlMode = StringUtils.isNotBlank(querySql);
			querySqlModeFlags.add(isQuerySqlMode);

			// both false
			if (!isTableMode && !isQuerySqlMode) {

				String bussinessMessage = "table and querySql should configured one item.";
				String message = StrUtil.buildOriginalCauseMessage(
						bussinessMessage, null);
				LOG.error(message);

				throw new DataXException(
						SqlServerReaderErrorCode.TABLE_QUERYSQL_MISSING,
						bussinessMessage);
			}
			// warn:both true
			if (isTableMode && isQuerySqlMode) {
				String bussinessMessage = "table and querySql can not mixed.";
				String message = StrUtil.buildOriginalCauseMessage(
						bussinessMessage, null);
				LOG.error(message);

				throw new DataXException(
						SqlServerReaderErrorCode.TABLE_QUERYSQL_MIXED,
						bussinessMessage);
			}
		}

		if (!ListUtil.checkIfValueSame(tableModeFlags)
				|| !ListUtil.checkIfValueSame(querySqlModeFlags)) {
			String bussinessMessage = "table and querySql can not mixed.";
			String message = StrUtil.buildOriginalCauseMessage(
					bussinessMessage, null);
			LOG.error(message);

			throw new DataXException(
					SqlServerReaderErrorCode.TABLE_QUERYSQL_MIXED,
					bussinessMessage);
		}

		return tableModeFlags.get(0);
	}

	// warn : 虽然sqlserver没有分库分表，但是配置也要和其他rds一致
	private static void dealJdbcAndTableConfig(Configuration originReaderConfig) {
		String username = originReaderConfig.getString(Key.USERNAME);
		String password = originReaderConfig.getString(Key.PASSWORD);

		boolean isTableMode = originReaderConfig
				.getBool(Constant.IS_TABLE_MODE);

		List<Object> conns = originReaderConfig.getList(Constant.CONN_MARK,
				Object.class);

		int tableNum = 0;

		for (int i = 0; i < conns.size(); i++) {
			Configuration connConf = Configuration
					.from(conns.get(i).toString());

			connConf.getNecessaryValue(Key.JDBC_URL,
					SqlServerReaderErrorCode.REQUIRED_VALUE);

			List<String> jdbcUrls = connConf
					.getList(Key.JDBC_URL, String.class);
			List<String> preSql = connConf.getList(Key.PRE_SQL, String.class);

			String jdbcUrl = DBUtil.chooseJdbcUrl(DataBaseType.SQLServer,
					jdbcUrls, username, password, preSql);

			// 回写到connection[i].jdbcUrl
			originReaderConfig.set(String.format("%s[%d].%s",
					Constant.CONN_MARK, i, Key.JDBC_URL), jdbcUrl);

			if (isTableMode) {
				// table 方式,对每一个connection 上配置的table 项进行解析(已对表名称进行了 [] 处理的)
				List<String> tables = connConf.getList(Key.TABLE, String.class);

				List<String> expandedTables = TableExpandUtil.expandTableConf(
						DataBaseType.SQLServer, tables);
				if (null == expandedTables || expandedTables.isEmpty()) {
					throw new DataXException(
							SqlServerReaderErrorCode.ILLEGAL_VALUE,
							"sql server read table config error.");
				}

				originReaderConfig.set(String.format("%s[%d].%s",
						Constant.CONN_MARK, i, Key.TABLE), expandedTables);

				tableNum += expandedTables.size();

			} else {
				// querySql mode
				LOG.info("user querySql mode");
			}
		}

		originReaderConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
	}

	private static void dealColumnConfig(Configuration originReaderConfig) {
		boolean isTableMode = originReaderConfig
				.getBool(Constant.IS_TABLE_MODE);
		List<String> userConfiguredColumns = originReaderConfig.getList(
				Key.COLUMN, String.class);

		if (isTableMode) {
			// don't have a column config
			if (null == userConfiguredColumns
					|| 0 == userConfiguredColumns.size()) {
				String businessMessage = "Lost column config.";
				String message = StrUtil.buildOriginalCauseMessage(
						businessMessage, null);

				LOG.error(message);
				throw new DataXException(SqlServerReaderErrorCode.REQUIRED_KEY,
						businessMessage);
			} else {
				// deal split pk quote
				String splitPk = originReaderConfig.getString(Key.SPLIT_PK,
						null);
				if (StringUtils.isNoneBlank(splitPk)) {
					if (splitPk.startsWith("[") && splitPk.endsWith("]")) {
						splitPk = splitPk.substring(1, splitPk.length() - 1)
								.toLowerCase();
					}
					originReaderConfig.set(Key.SPLIT_PK, TableExpandUtil
							.quoteTableOrColumnName(DataBaseType.SQLServer,
									splitPk));
				}

				if (1 == userConfiguredColumns.size()
						&& "*".equals(userConfiguredColumns.get(0))) {
					LOG.warn(SqlServerReaderErrorCode.NOT_RECOMMENDED
							.toString()
							+ ": because column configed as * may not work when you changed your table structure.");
					originReaderConfig.set(Key.COLUMN, "*");
				} else {
					String username = originReaderConfig
							.getString(Key.USERNAME);
					String password = originReaderConfig
							.getString(Key.PASSWORD);

					String jdbcUrl = originReaderConfig.getString(String
							.format("%s[0].%s", Constant.CONN_MARK,
									Key.JDBC_URL));

					String tableName = originReaderConfig.getString(String
							.format("%s[0].%s[0]", Constant.CONN_MARK,
									Key.TABLE));

					List<String> allColumns = DBUtil.getTableColumns(
							DataBaseType.SQLServer, jdbcUrl, username,
							password, tableName);

					if (LOG.isDebugEnabled()) {
						LOG.debug("table:[{}] has userConfiguredColumns:[{}].",
								tableName, StringUtils.join(allColumns, ","));
					}

					List<String> quotedColumns = new ArrayList<String>();

					for (String column : userConfiguredColumns) {
						if ("*".equals(column)) {
							throw new DataXException(
									SqlServerReaderErrorCode.ILLEGAL_VALUE,
									"no column named [*].");
						}

						if (allColumns.contains(column)) {
							quotedColumns.add(TableExpandUtil
									.quoteTableOrColumnName(
											DataBaseType.SQLServer, column));
						} else {
							// function
							quotedColumns.add(column);
						}
					}

					originReaderConfig.set(Key.COLUMN,
							StringUtils.join(quotedColumns, ","));

					if (StringUtils.isNotBlank(splitPk)) {

						if (!allColumns.contains(splitPk)) {
							String bussinessMessage = String.format(
									"No pk column named:[%s].", splitPk);
							String message = StrUtil.buildOriginalCauseMessage(
									bussinessMessage, null);
							LOG.error(message);

							throw new DataXException(
									SqlServerReaderErrorCode.ILLEGAL_SPLIT_PK,
									bussinessMessage);
						}
					}

				}
			}

		} else {
			// querySql模式，不希望配制 column，那样是混淆不清晰的
			if (null != userConfiguredColumns
					&& userConfiguredColumns.size() > 0) {
				LOG.warn(SqlServerReaderErrorCode.NOT_RECOMMENDED.toString()
						+ "because you have configed querySql, no need to config column.");
				originReaderConfig.remove(Key.COLUMN);
			}

			// querySql模式，不希望配制 where，那样是混淆不清晰的
			String where = originReaderConfig.getString(Key.WHERE);
			if (StringUtils.isNotBlank(where)) {
				LOG.warn(SqlServerReaderErrorCode.NOT_RECOMMENDED.toString()
						+ "because you have configed querySql, no need to config where.");
				originReaderConfig.remove(Key.WHERE);
			}
			// querySql模式，不希望配制 splitPk，那样是混淆不清晰的
			String splitPk = originReaderConfig.getString(Key.SPLIT_PK, null);
			if (StringUtils.isNotBlank(splitPk)) {
				LOG.warn(SqlServerReaderErrorCode.NOT_RECOMMENDED.toString()
						+ "because you have configured querySql, no need to config splitPk.");
				originReaderConfig.remove(Key.SPLIT_PK);
			}
		}

	}

}
