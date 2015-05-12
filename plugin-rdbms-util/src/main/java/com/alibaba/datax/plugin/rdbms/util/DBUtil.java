package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;

public final class DBUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);

    private DBUtil() {
    }

    public static String chooseJdbcUrl(final DataBaseType dataBaseType,
                                       final List<String> jdbcUrls, final String username,
                                       final String password, final List<String> preSql,
                                       final boolean checkSlave) {

        if (null == jdbcUrls || jdbcUrls.isEmpty()) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONF_ERROR,
                    String.format("您的jdbcUrl的配置信息有错, 因为jdbcUrl[%s]不能为空. 请检查您的配置并作出修改.",
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
                                        url, username, password, checkSlave);
                            }
                            if (connOK) {
                                return url;
                            }
                        }
                    }
                    throw new Exception("DataX无法连接对应的数据库，可能原因是：1) 配置的ip/port/database/jdbc错误，无法连接。2) 配置的username/password错误，鉴权失败。请和DBA确认该数据库的连接信息是否正确。");
                }
            }, 3, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("数据库连接失败. 因为根据您配置的连接信息,无法从:%s 中找到可连接的jdbcUrl. 请检查您的配置并作出修改.",
                            StringUtils.join(jdbcUrls, ",")), e);
        }

    }

    /**
     * 检查slave的库中的数据是否已到凌晨00:00
     * 如果slave同步的数据还未到00:00返回false
     * 否则范围true
     *
     * @author ZiChi
     * @version 1.0 2014-12-01
     */
    private static boolean isSlaveBehind(Connection conn) {
        try {
            ResultSet rs = query(conn, "SHOW VARIABLES LIKE 'read_only'");
            if (rs.next()) {
                String readOnly = rs.getString("Value");
                if ("ON".equalsIgnoreCase(readOnly)) { //备库
                    ResultSet rs1 = query(conn, "SHOW SLAVE STATUS");
                    if (rs1.next()) {
                        String ioRunning = rs1.getString("Slave_IO_Running");
                        String sqlRunning = rs1.getString("Slave_SQL_Running");
                        long secondsBehindMaster = rs1.getLong("Seconds_Behind_Master");
                        if ("Yes".equalsIgnoreCase(ioRunning) && "Yes".equalsIgnoreCase(sqlRunning)) {
                            ResultSet rs2 = query(conn, "SELECT TIMESTAMPDIFF(SECOND, CURDATE(), NOW())");
                            rs2.next();
                            long secondsOfDay = rs2.getLong(1);
                            return secondsBehindMaster > secondsOfDay;
                        } else {
                            return true;
                        }
                    } else {
                        LOG.warn("SHOW SLAVE STATUS has no result");
                    }
                }
            } else {
                LOG.warn("SHOW VARIABLES like 'read_only' has no result");
            }
        } catch (Exception e) {
            LOG.warn("checkSlave failed, errorMessage:[{}].", e.getMessage());
        }
        return false;
    }

    /**
     * 检查表是否具有insert 权限
     * insert on *.* 或者 insert on database.* 时验证通过
     * 当insert on database.tableName时，确保tableList中的所有table有insert 权限，验证通过
     * 其它验证都不通过
     *
     * @author ZiChi
     * @version 1.0 2015-01-28
     */
    public static boolean hasInsertPrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList) {
        /*准备参数*/
        String[] urls = jdbcURL.split("/");
        String dbName;
        if (urls != null && urls.length != 0) {
            dbName = urls[3];
        } else
            return false;

        String dbPattern = "`" + dbName + "`.*";
        Collection<String> tableNames = new HashSet<String>(tableList.size());
        tableNames.addAll(tableList);

        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        try {
            ResultSet rs = query(connection, "SHOW GRANTS FOR " + userName);
            while (rs.next()) {
                String grantRecord = rs.getString("Grants for " + userName + "@%");
                String[] params = grantRecord.split("\\`");
                if (params != null && params.length >= 3) {
                    String tableName = params[3];
                    if (!tableName.equals("*") && tableNames.contains(tableName))
                        tableNames.remove(tableName);
                } else {
                    if (grantRecord.contains("INSERT")) {
                        if (grantRecord.contains("*.*"))
                            return true;
                        else if (grantRecord.contains(dbPattern)) {
                            return true;
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Check the database has the Insert Privilege failed, errorMessage:[{}]", e.getMessage());
        }
        if (tableNames.isEmpty())
            return true;
        return false;
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
                    String.format("数据库连接失败. 因为根据您配置的连接信息:%s获取数据库连接失败. 请检查您的配置并作出修改.", jdbcUrl), e);
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
     * @param conn Database connection .
     * @param sql  sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize)
            throws SQLException {
        // make sure autocommit is off
        conn.setAutoCommit(false);
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

    public static List<String> getTableColumns(DataBaseType dataBaseType,
                                               String jdbcUrl, String user, String pass, String tableName) {
        Connection conn = getConnection(dataBaseType, jdbcUrl, user, pass);
        return getTableColumnsByConn(conn, tableName, "jdbcUrl:"+jdbcUrl);
    }

    public static List<String> getTableColumnsByConn(Connection conn, String tableName, String basicMsg) {
        List<String> columns = new ArrayList<String>();
        Statement statement = null;
        ResultSet rs = null;
        try {
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
                            String.format("获取字段信息失败. 根据您的配置信息，获取表的所有字段名称时失败. 该错误可能是由于配置错误导致，请检查您的配置信息. 错误配置信息上下文: %s,table:[%s]", basicMsg, tableName), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, conn);
        }

        return columns;
    }

    /**
     * @return Left:ColumnName Middle:ColumnType Right:ColumnTypeName
     */
    public static Triple<List<String>, List<Integer>, List<String>> getColumnMetaData(
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

    /**
     * @return Left:ColumnName Middle:ColumnType Right:ColumnTypeName
     */
    public static Triple<List<String>, List<Integer>, List<String>> getColumnMetaData(
            Connection conn, String tableName, String column) {
        Statement statement = null;
        ResultSet rs = null;

        Triple<List<String>, List<Integer>, List<String>> columnMetaData = new ImmutableTriple<List<String>, List<Integer>, List<String>>(
                new ArrayList<String>(), new ArrayList<Integer>(),
                new ArrayList<String>());
        try {
            statement = conn.createStatement();
            String queryColumnSql = "select " + column + " from " + tableName
                    + " where 1=2";

            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {

                columnMetaData.getLeft().add(rsMetaData.getColumnName(i + 1));
                columnMetaData.getMiddle().add(rsMetaData.getColumnType(i + 1));
                columnMetaData.getRight().add(
                        rsMetaData.getColumnTypeName(i + 1));
            }
            return columnMetaData;

        } catch (SQLException e) {
            throw DataXException
                    .asDataXException(DBUtilErrorCode.GET_COLUMN_INFO_FAILED,
                            String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.", tableName), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, null);
        }
    }

    public static boolean testConnWithoutRetry(DataBaseType dataBaseType,
                                               String url, String user, String pass, boolean checkSlave) {
        Connection connection = null;

        try {
            connection = connect(dataBaseType, url, user, pass);
            if (connection != null) {
                if (dataBaseType.equals(dataBaseType.MySql) && checkSlave) {
                    //dataBaseType.MySql
                    boolean connOk = !isSlaveBehind(connection);
                    return connOk;
                } else {
                    return true;
                }
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

    // warn:until now, only oracle need to handle session config.
    public static void dealWithSessionConfig(Connection conn,
                                             Configuration config, DataBaseType databaseType, String message) {
        List<String> sessionConfig = null;
        switch (databaseType) {
            case Oracle:
                sessionConfig = config.getList(Key.SESSION,
                        new ArrayList<String>(), String.class);
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            case DRDS:
                // 用于关闭 drds 的分布式事务开关
                sessionConfig = new ArrayList<String>();
                sessionConfig.add("set transaction policy 4");
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            case MySql:
                sessionConfig = config.getList(Key.SESSION,
                        new ArrayList<String>(), String.class);
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            default:
                break;
        }
    }

    private static void doDealWithSessionConfig(Connection conn,
                                                List<String> sessions, String message) {
        if (null == sessions || sessions.isEmpty()) {
            return;
        }

        Statement stmt;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            throw DataXException
                    .asDataXException(DBUtilErrorCode.SET_SESSION_ERROR, String
                                    .format("session配置有误. 因为根据您的配置执行 session 设置失败. 上下文信息是:[%s]. 请检查您的配置并作出修改.", message),
                            e);
        }

        for (String sessionSql : sessions) {
            LOG.info("execute sql:[{}]", sessionSql);
            try {
                DBUtil.executeSqlWithoutResultSet(stmt, sessionSql);
            } catch (SQLException e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.SET_SESSION_ERROR, String.format(
                                "session配置有误. 因为根据您的配置执行 session 设置失败. 上下文信息是:[%s]. 请检查您的配置并作出修改.", message), e);
            }
        }
        DBUtil.closeDBResources(stmt, null);
    }
}
