package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RetryUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public final class DBUtil {
	private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);

	private DBUtil() {
	}

	public static String chooseJdbcUrl(final DataBaseType dataBaseType,
			final List<String> jdbcUrls, final String username,
			final String password, final List<String> preSql) {
		if (null == jdbcUrls || jdbcUrls.isEmpty()) {
			throw DataXException.asDataXException(
					DBUtilErrorCode.CONF_ERROR,
					String.format("jdbcURL in [%s] 不能为空.",
							StringUtils.join(jdbcUrls, ",")));
		}

		try {
			return RetryUtil.executeWithRetry(new Callable<String>() {

				@Override
				public String call() throws Exception {
					boolean connOK = false;
					for (String url : jdbcUrls) {
						if (StringUtils.isNotBlank(url)) {
							url = url.trim();
							if (null != preSql && !preSql.isEmpty()) {
								connOK = testConnWithoutRetry(dataBaseType,
										url, username, password, preSql);
							} else {
								connOK = testConnWithoutRetry(dataBaseType,
										url, username, password);
							}
							if (connOK) {
								return url;
							}
						}
					}
					throw new Exception("No available jdbcURL yet.");
				}
			}, 3, 1000L, true);
		} catch (Exception e) {
			throw DataXException.asDataXException(
					DBUtilErrorCode.CONN_DB_ERROR,
					String.format("无法从:%s 中找到可连接的jdbcURL.",
							StringUtils.join(jdbcUrls, ",")), e);
		}

	}

	/**
	 * Get direct JDBC connection
	 * <p/>
	 * if connecting failed, try to connect for MAX_TRY_TIMES times
	 * <p/>
	 * NOTE: In DataX, we don't need connection pool in fact
	 */
	public static Connection getConnection(final DataBaseType dataBaseType,
			final String jdbcUrl, final String username, final String password) {

		try {
			return RetryUtil.executeWithRetry(new Callable<Connection>() {
				@Override
				public Connection call() throws Exception {
					return DBUtil.connect(dataBaseType, jdbcUrl, username,
							password);
				}
			}, Constant.MAX_TRY_TIMES, 1000L, true);
		} catch (Exception e) {
			throw DataXException.asDataXException(
					DBUtilErrorCode.CONN_DB_ERROR,
					String.format("获取数据库连接失败. 连接信息是:%s .", jdbcUrl), e);
		}

	}

	private static synchronized Connection connect(DataBaseType dataBaseType,
			String url, String user, String pass) {
		try {
			Class.forName(dataBaseType.getDriverClassName());
			DriverManager.setLoginTimeout(Constant.TIMEOUT_SECONDS);
			return DriverManager.getConnection(url, user, pass);
		} catch (Exception e) {
			throw DataXException.asDataXException(
					DBUtilErrorCode.CONN_DB_ERROR, e);
		}
	}

	/**
	 * a wrapped method to execute select-like sql statement .
	 *
	 * @param conn
	 *            Database connection .
	 * @param sql
	 *            sql statement to be executed
	 * @return a {@link ResultSet}
	 * @throws SQLException
	 *             if occurs SQLException.
	 */
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
	 * @param sql
	 *            sql statement to be executed
	 * @return a {@link ResultSet}
	 * @throws SQLException
	 *             if occurs SQLException.
	 */
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
	 * @throws IllegalArgumentException
	 */
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

	public static List<String> getTableColumns(DataBaseType dataBaseType,
			String jdbcUrl, String user, String pass, String tableName) {
		List<String> columns = new ArrayList<String>();
		Connection conn = null;
		Statement statement = null;
		ResultSet rs = null;
		try {
			conn = getConnection(dataBaseType, jdbcUrl, user, pass);
			statement = conn.createStatement();
			String queryColumnSql = String.format("select * from %s where 1=2",
					tableName);
			rs = statement.executeQuery(queryColumnSql);
			ResultSetMetaData rsMetaData = rs.getMetaData();
			for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
				columns.add(rsMetaData.getColumnName(i + 1));
			}

		} catch (SQLException e) {
			throw DataXException
					.asDataXException(DBUtilErrorCode.GET_COLUMN_INFO_FAILED,
							"获取表的所有字段名称时失败.", e);
		} finally {
			DBUtil.closeDBResources(rs, statement, conn);
		}

		return columns;
	}

	public static ResultSetMetaData getColumnMetaData(
			DataBaseType dataBaseType, String jdbcUrl, String user,
			String pass, String tableName, String column) {
		Connection conn = null;
		try {
			conn = getConnection(dataBaseType, jdbcUrl, user, pass);
			return getColumnMetaData(conn, tableName, column);
		} finally {
			DBUtil.closeDBResources(null, null, conn);
		}
	}

	public static ResultSetMetaData getColumnMetaData(Connection conn,
			String tableName, String column) {
		Statement statement = null;
		ResultSet rs = null;
		try {
			statement = conn.createStatement();
			String queryColumnSql = "select " + column + " from " + tableName
					+ " where 1=2";

			rs = statement.executeQuery(queryColumnSql);
			ResultSetMetaData rsMetaData = rs.getMetaData();
			return rsMetaData;

		} catch (SQLException e) {
			throw DataXException
					.asDataXException(DBUtilErrorCode.GET_COLUMN_INFO_FAILED,
							"获取表的字段的元信息时失败.", e);
		} finally {
			// 注意：不关闭这两个资源,在关闭Connection时，会统一释放Statement,ResultSet的资源，目的是保证ResultSetMetaData的后续可用
			// DBUtil.closeDBResources(rs, statement, null);
		}
	}

	public static boolean testConnWithoutRetry(DataBaseType dataBaseType,
			String url, String user, String pass) {
		Connection connection = null;
		try {
			connection = connect(dataBaseType, url, user, pass);
			if (null != connection) {
				return true;
			}
		} catch (Exception e) {
			LOG.warn("test connection of [{}] failed, for {}.", url,
					e.getMessage());
		} finally {
			DBUtil.closeDBResources(null, connection);
		}

		return false;
	}

	public static boolean testConnWithoutRetry(DataBaseType dataBaseType,
			String url, String user, String pass, List<String> preSql) {
		Connection connection = null;
		try {
			connection = connect(dataBaseType, url, user, pass);
			if (null != connection) {
				for (String pre : preSql) {
					if (doPreCheck(connection, pre) == false) {
						LOG.warn("doPreCheck failed.");
						return false;
					}
				}
				return true;
			}
		} catch (Exception e) {
			LOG.warn("test connection of [{}] failed, for {}.", url,
					e.getMessage());
		} finally {
			DBUtil.closeDBResources(null, connection);
		}

		return false;
	}

	public static ResultSet query(Connection conn, String sql)
			throws SQLException {
		Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		return query(stmt, sql);
	}

	private static boolean doPreCheck(Connection conn, String pre) {
		ResultSet rs = null;
		try {
			rs = query(conn, pre);

			int checkResult = -1;
			if (rs.next()) {
				checkResult = rs.getInt(1);
				if (rs.next()) {
					LOG.warn(
							"pre check failed. It should return one result:0, pre:[{}].",
							pre);
					return false;
				}

			}

			if (0 == checkResult) {
				return true;
			}

			LOG.warn(
					"pre check failed. It should return one result:0, pre:[{}].",
					pre);
		} catch (Exception e) {
			LOG.warn("pre check failed. pre:[{}], errorMessage:[{}].", pre,
					e.getMessage());
		} finally {
			DBUtil.closeResultSet(rs);
		}
		return false;
	}

}
