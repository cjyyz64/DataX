package com.alibaba.datax.plugin.reader.sqlserverreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.TableExpandUtil;
import com.alibaba.datax.plugin.reader.sqlserverreader.Constants;
import com.alibaba.datax.plugin.reader.sqlserverreader.Key;
import com.alibaba.datax.plugin.reader.sqlserverreader.SqlServerReaderErrorCode;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class ConfigPretreatUtil {
	private static Logger LOG = LoggerFactory
			.getLogger(ConfigPretreatUtil.class);

	/**
	 * 验证原始DataX sqlserverreader 配置合法性，预处理
	 * 
	 * @param originReaderConfig
	 *            original reader config
	 * */
	public static void validate(Configuration originReaderConfig) {
		originReaderConfig.getNecessaryValue(Key.USERNAME,
				SqlServerReaderErrorCode.CONF_ERROR);
		originReaderConfig.getNecessaryValue(Key.PASSWORD,
				SqlServerReaderErrorCode.CONF_ERROR);

		int fetchSize = originReaderConfig.getInt(Key.FETCH_SIZE,
				Constants.DEFAULT_FETCH_SIZE);
		if (fetchSize < 1) {
			throw new DataXException(SqlServerReaderErrorCode.CONF_ERROR,
					"fetchSize can not less than 1.");
		}
		originReaderConfig.set(Key.FETCH_SIZE, fetchSize);

		// connect part
		List<Object> conns = originReaderConfig.getList(Constants.CONNECTION,
				Object.class);

		// warn:to sql server ,connect part array size must be 1
		if (null == conns || 1 != conns.size()) {
			throw new DataXException(SqlServerReaderErrorCode.CONF_ERROR,
					"connection configuration part invalid, shall have one and only one element");
		}

		boolean isTableMode = recognizeTableOrQuerySqlMode(originReaderConfig);
		originReaderConfig.set(Constants.TABLE_MODE, isTableMode);

		dealTableConfig(originReaderConfig);

		dealColumnConfig(originReaderConfig);
	}

	// resolve table mode or querySql mode
	private static boolean recognizeTableOrQuerySqlMode(
			Configuration originReaderConfig) {

		List<Object> conns = originReaderConfig.getList(Constants.CONNECTION,
				Object.class);

		List<Boolean> tableModeFlag = new ArrayList<Boolean>();
		List<Boolean> querySqlModeFlag = new ArrayList<Boolean>();

		String table;
		String querySql;
		for (int i = 0; i < conns.size(); i++) {
			Configuration connConf = Configuration
					.from(conns.get(i).toString());

			table = connConf.getString(Key.TABLE);
			querySql = connConf.getString(Key.QUERYSQL);

			tableModeFlag.add(StringUtils.isNotBlank(table));
			querySqlModeFlag.add(StringUtils.isNoneBlank(querySql));

			// both false
			if (!tableModeFlag.get(i).booleanValue()
					&& !querySqlModeFlag.get(i)) {
				throw new DataXException(
						SqlServerReaderErrorCode.TABLE_QUERYSQL_MIXED,
						"table and querySql must have one.");
			}
			// warn:both true
			if (tableModeFlag.get(i).booleanValue() && querySqlModeFlag.get(i)) {
				throw new DataXException(
						SqlServerReaderErrorCode.TABLE_QUERYSQL_MIXED,
						"table and querySql can only have one.");
			}
		}

		if (!isListValueSame(tableModeFlag)
				|| !isListValueSame(querySqlModeFlag)) {
			throw new DataXException(
					SqlServerReaderErrorCode.TABLE_QUERYSQL_MIXED,
					"table and querySql can not mixed.");
		}

		return tableModeFlag.get(0);
	}

	private static boolean isListValueSame(List<Boolean> flags) {
		if (1 == flags.size()) {
			return true;
		}

		boolean preValue = flags.get(0);
		for (int i = 1; i < flags.size(); i++) {
			if (preValue != flags.get(i)) {
				return false;
			}
			preValue = flags.get(i);
		}
		return true;
	}

	// warn : 虽然sqlserver没有分库分表，但是配置也要和其他rds一致
	private static void dealTableConfig(Configuration originReaderConfig) {
		String username = originReaderConfig.getString(Key.USERNAME);
		String password = originReaderConfig.getString(Key.PASSWORD);

		boolean isTableMode = originReaderConfig.getBool(Constants.TABLE_MODE);

		List<Object> conns = originReaderConfig.getList(Constants.CONNECTION,
				Object.class);

		int tableCount = 0;

		for (int i = 0; i < conns.size(); i++) {
			Configuration connConf = Configuration
					.from(conns.get(i).toString());

			connConf.getNecessaryValue(Key.JDBC_URL,
					SqlServerReaderErrorCode.CONF_ERROR);

			List<String> jdbcUrls = connConf
					.getList(Key.JDBC_URL, String.class);
			String jdbcUrl = checkJdbcUrl(jdbcUrls, username, password);

			// warn : path 写回到配置
			originReaderConfig.set(String.format("%s[%d].%s",
					Constants.CONNECTION, i, Key.JDBC_URL), jdbcUrl);

			if (isTableMode) {
				List<String> tables = connConf.getList(Key.TABLE, String.class);

				List<String> expandedTables = TableExpandUtil.expandTableConf(
						DataBaseType.SQLServer, tables);
				if (null == expandedTables || expandedTables.isEmpty()) {
					throw new DataXException(
							SqlServerReaderErrorCode.CONF_ERROR,
							"sql server read table config error.");
				}

				originReaderConfig.set(String.format("%s[%d].%s",
						Constants.CONNECTION, i, Key.TABLE), expandedTables);

				tableCount += expandedTables.size();

			} else {
				// querySql mode
				LOG.info("user querySql mode");
			}
		}

		originReaderConfig.set(Constants.TABLE_NUMBER, tableCount);
	}

	private static void dealColumnConfig(Configuration originReaderConfig) {
		boolean isTableMode = originReaderConfig.getBool(Constants.TABLE_MODE);
		List<String> columns = originReaderConfig.getList(Key.COLUMN,
				String.class);
		//
		if (isTableMode) {
			// don't have a column config, default all columns
			if (null == columns || 0 == columns.size()) {
				originReaderConfig.set(Key.COLUMN, "*");
				LOG.warn(SqlServerReaderErrorCode.NOT_RECOMMENDED.toString()
						+ ": because column configed as empty may not work when you changed your table structure.");
				throw new DataXException(SqlServerReaderErrorCode.CONF_ERROR,
						"columns could not be empty or blank.");
			} else if (1 == columns.size() && "*".equals(columns.get(0))) {
				LOG.warn(SqlServerReaderErrorCode.NOT_RECOMMENDED.toString()
						+ ": because column configed as * may not work when you changed your table structure.");
				originReaderConfig.set(Key.COLUMN, "*");
			} else {
				// TODO ignore case or not for column name
				// warn : at connect segment
				String username = originReaderConfig.getString(Key.USERNAME);
				String password = originReaderConfig.getString(Key.PASSWORD);

				String jdbcUrl = originReaderConfig.getString(String.format(
						"%s[0].%s", Constants.CONNECTION, Key.JDBC_URL));

				// 每一个table的结构都相同
				String tableName = originReaderConfig.getString(String.format(
						"%s[0].%s[0]", Constants.CONNECTION, Key.TABLE));

				List<String> allColumns = DBUtil.getTableColumns(
						DataBaseType.SQLServer, jdbcUrl, username, password,
						tableName);

				List<String> quotedColumns = new ArrayList<String>();

				for (String column : columns) {
					if ("*".equals(column)) {
						throw new DataXException(
								SqlServerReaderErrorCode.CONF_ERROR,
								"no column named [*].");
					}

					if (allColumns.contains(column)) {
						quotedColumns.add(TableExpandUtil
								.quoteTableOrColumnName(DataBaseType.SQLServer,
										column));
					} else {
						// function
						quotedColumns.add(column);
					}
				}

				originReaderConfig.set(Key.COLUMN,
						StringUtils.join(quotedColumns, ","));

			}

		} else {
			// querySql mode , Configure column is not recommended
			if (null != columns && columns.size() > 0) {
				LOG.warn(SqlServerReaderErrorCode.NOT_RECOMMENDED.toString()
						+ "because you have configed querySql, no need to config column.");
				originReaderConfig.remove(Key.COLUMN);
			}

			// querySql mode, Configure where is not recommended
			String where = originReaderConfig.getString(Key.WHERE);
			if (StringUtils.isNotBlank(where)) {
				LOG.warn(SqlServerReaderErrorCode.NOT_RECOMMENDED.toString()
						+ "because you have configed querySql, no need to config where.");
				originReaderConfig.remove(Key.WHERE);
			}
		}

	}

	private static String checkJdbcUrl(List<String> jdbcUrls, String username,
			String password) {
		Connection conn = null;
		for (String jdbcUrl : jdbcUrls) {
			try {
				conn = DBUtil.getConnection(DataBaseType.SQLServer, jdbcUrl,
						username, password);
			} catch (Exception e) {
				LOG.warn("jdbcUrl:[{}] not available.", jdbcUrl);
			}
			if (null != conn) {
				return jdbcUrl;
			}
		}
		throw new DataXException(SqlServerReaderErrorCode.CONN_DB_ERROR,
				"can not connect to");
	}

}
