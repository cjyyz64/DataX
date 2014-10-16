package com.alibaba.datax.plugin.rdbms.util;

import java.util.HashMap;
import java.util.Map;

/**
 * refer:http://blog.csdn.net/ring0hx/article/details/6152528
 * <p/>
 * 是否需要添加版本信息？
 */
public enum DataBaseType {
    MySql("mysql", "com.mysql.jdbc.Driver"),
    Oracle("oracle", "oracle.jdbc.OracleDriver"),
    SQLServer("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    PostgreSQL("postgresql", "org.postgresql.Driver"),
    Sybase("sybase", "com.sybase.jdbc2.jdbc.SybDriver (com.sybase.jdbc3.jdbc.SybDriver)"),
    DB2("db2", "com.ibm.db2.jcc.DB2Driver"),;

    private String typeName;
    private String driverClassName;

    DataBaseType(String typeName, String driverClassName) {
        this.typeName = typeName;
        this.driverClassName = driverClassName;
    }

    public String getTypeName() {
        return this.typeName;
    }

    public String getDriverClassName() {
        return this.driverClassName;
    }

    public static Map<String, String> getTypeAndDriverMap() {
        Map<String, String> result = new HashMap<String, String>();

        DataBaseType[] values = DataBaseType.values();
        for (DataBaseType each : values) {
            result.put(each.getTypeName(), each.getDriverClassName());
        }
        return result;
    }
}
