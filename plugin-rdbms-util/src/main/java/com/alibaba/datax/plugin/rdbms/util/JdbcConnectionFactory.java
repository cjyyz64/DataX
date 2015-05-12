package com.alibaba.datax.plugin.rdbms.util;

import java.sql.Connection;

/**
 * Date: 15/3/16 下午3:12
 *
 * @author liupeng <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class JdbcConnectionFactory implements ConnectionFactory {

    private DataBaseType dataBaseType;

    private String jdbcUrl;

    private String userName;

    private String password;

    public JdbcConnectionFactory(DataBaseType dataBaseType, String jdbcUrl, String userName, String password) {
        this.dataBaseType = dataBaseType;
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public Connection getConnecttion() {
        return DBUtil.getConnection(dataBaseType, jdbcUrl, userName, password);
    }

    @Override
    public String getConnectionInfo() {
        return "jdbcUrl:" + jdbcUrl;
    }
}
