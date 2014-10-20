package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RetryUtil;
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

    static class ConnectionOwner implements Callable<Connection> {
        private DataBaseType dataBaseType;
        private String jdbcUrl;
        private String username;
        private String password;

        ConnectionOwner(DataBaseType dataBaseType, String jdbcUrl, String username, String password) {
            this.dataBaseType = dataBaseType;
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
        }

        @Override
        public Connection call() throws Exception {
            return DBUtil.connect(dataBaseType, jdbcUrl, username, password);
        }
    }

    /**
     * Get direct JDBC connection
     * <p/>
     * if connecting failed, try to connect for MAX_TRY_TIMES times
     * <p/>
     * NOTE: In DataX, we don't need connection pool in fact
     */
    public static Connection getConnection(DataBaseType dataBaseType, String jdbcUrl,
                                           String username, String password) {

        try {
            return RetryUtil.executeWithRetry(new ConnectionOwner(dataBaseType, jdbcUrl,
                    username, password), Constant.MAX_TRY_TIMES, 1000L, true);
        } catch (Exception e) {
            throw new DataXException(DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("get jdbc connection failed, connection detail is [\n%s\n].",
                            jdbcUrl), e);
        }

    }

    private static synchronized Connection connect(DataBaseType dataBaseType, String url, String user,
                                                   String pass) {
        try {
            Class.forName(dataBaseType.getDriverClassName());
            DriverManager.setLoginTimeout(Constant.TIMEOUT_SECONDS);
            return DriverManager.getConnection(url, user, pass);
        } catch (Exception e) {
            throw new DataXException(DBUtilErrorCode.CONN_DB_ERROR, e);
        }
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn Database connection .
     * @param sql  sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
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
     * @param stmt {@link Statement}
     * @param sql  sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
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
     * @param rs {@link ResultSet} to be closed
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

    public static List<String> getMysqlTableColumns(String jdbcUrl,
                                                    String user, String pass, String tableName) {
        List<String> columns = new ArrayList<String>();
        Connection conn = getConnection(DataBaseType.MySql, jdbcUrl, user, pass);
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

    public static List<String> getTableColumns(DataBaseType dataBaseType, String jdbcUrl,
                                               String user, String pass, String tableName) {
        List<String> columns = new ArrayList<String>();
        Connection conn = getConnection(dataBaseType, jdbcUrl, user, pass);
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            // String dbName = getDBNameFromJdbcUrl(jdbcUrl);
            String dbName = conn.getCatalog(); // 获取数据库名databaseName
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
        // warn：如果是sql server，此方法有问题
        // if(jdbcUrl.trim().startsWith("jdbc:sqlserver:")){
        //
        // }
        // 如果是mysql
        int jdbcUrlBeginIndex = jdbcUrl.lastIndexOf("/") + 1;
        int tempEndIndex = jdbcUrl.indexOf("?");
        int jdbcUrlEndIndex = -1 == tempEndIndex ? jdbcUrl.length()
                : tempEndIndex;
        return jdbcUrl.substring(jdbcUrlBeginIndex, jdbcUrlEndIndex);
    }


    public static boolean testConnWithoutRetry(DataBaseType dataBaseType, String url, String user, String pass) {
        try {
            Connection connection = connect(dataBaseType, url, user, pass);
            if (null != connection) {
                return true;
            }
        } catch (Exception e) {
            LOG.warn("test connection of [{}] failed, for {}.", url,
                    e.getMessage());
        }

        return false;
    }

    public static boolean testConnWithoutRetry(DataBaseType dataBaseType, String url, String user,
                                               String pass, List<String> preSql) {
        try {
            Connection connection = connect(dataBaseType, url, user, pass);
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
        try {
            ResultSet rs = query(conn, pre);

            int checkResult = -1;
            if (rs.next()) {
                checkResult = rs.getInt(1);
                if (rs.next()) {
                    LOG.warn("pre check failed. It should return one result:0, pre:[{}].", pre);
                    return false;
                }

            }

            if (0 == checkResult) {
                return true;
            }

            LOG.warn("pre check failed. It should return one result:0, pre:[{}].", pre);
        } catch (Exception e) {
            LOG.warn("pre check failed. pre:[{}], errorMessage:[{}].", pre, e.getMessage());
        }
        return false;
    }
}
