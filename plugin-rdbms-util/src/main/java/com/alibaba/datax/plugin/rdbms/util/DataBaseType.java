package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.exception.DataXException;

/**
 * refer:http://blog.csdn.net/ring0hx/article/details/6152528
 * <p/>
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

    public String getDriverClassName() {
        return this.driverClassName;
    }

    public String appendJDBCSuffix(String jdbc) {
        String result = jdbc;
        switch (this) {
            case MySql:
                String suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull";
                if (jdbc.contains("?")) {
                    result = jdbc + "&" + suffix;
                } else {
                    result = jdbc + "?" + suffix;
                }
                break;
            case Oracle:
                break;
            case SQLServer:
                break;
            case DB2:
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type.");
        }

        return result;
    }

    public String formatPk(String splitPk) {
        String result = splitPk;

        switch (this) {
            case MySql:
            case Oracle:
                if (splitPk.length() >= 2 && splitPk.startsWith("`") && splitPk.endsWith("`")) {
                    result = splitPk.substring(1, splitPk.length() - 1).toLowerCase();
                }
                break;
            case SQLServer:
                if (splitPk.length() >= 2 && splitPk.startsWith("[") && splitPk.endsWith("]")) {
                    result = splitPk.substring(1, splitPk.length() - 1).toLowerCase();
                }
                break;
            case DB2:
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type.");
        }

        return result;
    }


    public String quoteColumnName(String columnName) {
        String result = columnName;

        switch (this) {
            case MySql:
                result = "`" + columnName.replace("`", "``") + "`";
                break;
            case Oracle:
                break;
            case SQLServer:
                result = "[" + columnName + "]";
                break;
            case DB2:
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type");
        }

        return result;
    }

    public String quoteTableName(String tableName) {
        String result = tableName;

        switch (this) {
            case MySql:
                result = "`" + tableName.replace("`", "``") + "`";
                break;
            case Oracle:
                break;
            case SQLServer:
                break;
            case DB2:
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type");
        }

        return result;
    }

}
