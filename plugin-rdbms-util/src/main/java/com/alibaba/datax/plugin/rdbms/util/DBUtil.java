package com.alibaba.datax.plugin.rdbms.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;

public final class DBUtil {
	private DBUtil() {
	}

	private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);

	private static final int TIMEOUT_SECONDS = 3;

	private static final int MAX_TRY_TIMES = 4;

	private static Map<String, String> CONNECTION_TYPE = new HashMap<String, String>();

	static {
		CONNECTION_TYPE.put("mysql", "com.mysql.jdbc.Driver");
		CONNECTION_TYPE.put("oracle", "oracle.jdbc.OracleDriver");
		CONNECTION_TYPE.put("sqlserver",
				"com.microsoft.sqlserver.jdbc.SQLServerDriver");
	}

	/**
	 * Get driver class
	 * */
	private static String getDriverClassName(final String type) {
		String driverClassName = CONNECTION_TYPE.get(type);

		if (!StringUtils.isBlank(driverClassName)) {
			return driverClassName;
		}

		throw new IllegalArgumentException(String.format(
				"Driver type [%s] not registered .", type));
	}

	/**
	 * Get direct JDBC connection
	 * 
	 * if connecting failed, try to connect for 3 times
	 * 
	 * NOTE: In DataX, we don't need connection pool in fact
	 * 
	 * @param jdbc
	 *            jdbc url
	 * 
	 * @param username
	 * 
	 * @param password
	 * 
	 * */
	public static Connection getConnection(final String type, final String url,
			final String user, final String pass) {
		Exception saveException = null;
		for (int tryTime = 0; tryTime < MAX_TRY_TIMES; tryTime++) {
			try {
				Connection connection = DBUtil.connect(
						DBUtil.getDriverClassName(type), url, user, pass);
				if (null != connection) {
					return connection;
				}
			} catch (Exception e) {
				LOG.warn("Connect to {} failed, for {}.", url, e.getMessage());
				saveException = e;
				try {
					Thread.sleep(1000L * (long) Math.pow(2, tryTime));
				} catch (InterruptedException unused) {
				}
				continue;
			}
		}

		if (saveException == null) {
			throw new DataXException(
					DBUtilErrorCode.CONN_DB_ERROR,
					String.format(
							"get jdbc connection failed, connection detail is [\n%s\n].",
							url));
		}

		throw new DataXException(DBUtilErrorCode.CONN_DB_ERROR, saveException);
	}

	private static synchronized Connection connect(
			final String driverClassName, final String url, final String user,
			final String pass) {
		try {
			Class.forName(driverClassName);
			DriverManager.setLoginTimeout(TIMEOUT_SECONDS);
			return DriverManager.getConnection(url, user, pass);
		} catch (Exception e) {
			throw new DataXException(DBUtilErrorCode.CONN_DB_ERROR, e);
		}
	}

	/**
	 * a wrapped method to execute select-like sql statement .
	 * 
	 * @param conn
	 *            Database connection .
	 * 
	 * @param sql
	 *            sql statement to be executed
	 * 
	 * @return a {@link ResultSet}
	 * 
	 * @throws SQLException
	 *             if occurs SQLException.
	 * 
	 * */
	public static ResultSet query(Connection conn, String sql, int fetchSize)
			throws SQLException {
		Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(fetchSize);
		return query(stmt, sql);
	}

	/**
	 * a wrapped method to execute select-like sql statement .
	 * 
	 * @param stmt
	 *            {@link Statement}
	 * 
	 * @param sql
	 *            sql statement to be executed
	 * 
	 * @return a {@link ResultSet}
	 * 
	 * @throws SQLException
	 *             if occurs SQLException.
	 * 
	 * */
	public static ResultSet query(Statement stmt, String sql)
			throws SQLException {
		return stmt.executeQuery(sql);
	}

	public static void executeSqlWithoutResultSet(Statement stmt, String sql)
			throws SQLException {
		stmt.execute(sql);
	}

	/**
	 * Close {@link ResultSet}, {@link Statement} referenced by this
	 * {@link ResultSet}
	 * 
	 * @param rs
	 *            {@link ResultSet} to be closed
	 * 
	 * @throws IllegalArgumentException
	 * 
	 * */
	public static void closeResultSet(ResultSet rs) {
		try {
			if (null != rs) {
				Statement stmt = rs.getStatement();
				if (null != stmt) {
					stmt.close();
					stmt = null;
				}
				rs.close();
			}
			rs = null;
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	public static void closeDBResources(ResultSet rs, Statement stmt,
			Connection conn) {
		if (null != rs) {
			try {
				rs.close();
			} catch (SQLException unused) {
			}
		}

		if (null != stmt) {
			try {
				stmt.close();
			} catch (SQLException unused) {
			}
		}

		if (null != conn) {
			try {
				conn.close();
			} catch (SQLException unused) {
			}
		}
	}

	public static void closeDBResources(Statement stmt, Connection conn) {
		closeDBResources(null, stmt, conn);
	}

	public static List<String> getMysqlTableColumns(String jdbcUrl,
			String user, String pass, String tableName) {
		List<String> columns = new ArrayList<String>();
		Connection conn = getConnection("mysql", jdbcUrl, user, pass);
		try {
			DatabaseMetaData databaseMetaData = conn.getMetaData();
			String dbName = getDBNameFromJdbcUrl(jdbcUrl);

			ResultSet rs = databaseMetaData.getColumns(dbName, null, tableName,
					"%");

			String tempColumn = null;
			while (rs.next()) {
				tempColumn = rs.getString("COLUMN_NAME");

				columns.add(tempColumn);
			}

		} catch (SQLException e) {
			throw new DataXException(DBUtilErrorCode.CONN_DB_ERROR, e);
		}
		return columns;

	}

	public static List<String> getTableColumns(String dbType,
			String jdbcUrl, String user, String pass, String tableName) {
		List<String> columns = new ArrayList<String>();
		Connection conn = getConnection(dbType, jdbcUrl, user, pass);
		try {
			DatabaseMetaData databaseMetaData = conn.getMetaData();
			String dbName = getDBNameFromJdbcUrl(jdbcUrl);

			ResultSet rs = databaseMetaData.getColumns(dbName, null, tableName,
					"%");

			String tempColumn = null;
			while (rs.next()) {
				tempColumn = rs.getString("COLUMN_NAME");

				columns.add(tempColumn);
			}

		} catch (SQLException e) {
			throw new DataXException(DBUtilErrorCode.CONN_DB_ERROR, e);
		}
		return columns;

	}

	private static String getDBNameFromJdbcUrl(String jdbcUrl) {
		int jdbcUrlBeginIndex = jdbcUrl.lastIndexOf("/") + 1;
		int tempEndIndex = jdbcUrl.indexOf("?");
		int jdbcUrlEndIndex = -1 == tempEndIndex ? jdbcUrl.length()
				: tempEndIndex;
		return jdbcUrl.substring(jdbcUrlBeginIndex, jdbcUrlEndIndex);
	}
}
