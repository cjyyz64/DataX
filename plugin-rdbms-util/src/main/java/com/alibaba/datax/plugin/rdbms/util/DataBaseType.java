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

    public String getTypeName() {
        return this.typeName;
    }

    public String getDriverClassName() {
        return this.driverClassName;
    }

    public String appendJDBCSuffix(String jdbc) {
        switch (this) {
            case MySql:
                String suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull";
                if (jdbc.contains("?")) {
                    return jdbc + "&" + suffix;
                } else {
                    return jdbc + "?" + suffix;
                }
            case Oracle:
                return jdbc;
            case SQLServer:
                return jdbc;
            default:
                throw new DataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type.");
        }
    }

    public String formatPk(String splitPk) {
        switch (this) {
            case MySql:
            case Oracle:
                if (splitPk.length() >= 2 && splitPk.startsWith("`") && splitPk.endsWith("`")) {
                    return splitPk.substring(1, splitPk.length() - 1).toLowerCase();
                }
            case SQLServer:
                if (splitPk.length() >= 2 && splitPk.startsWith("[") && splitPk.endsWith("]")) {
                    return splitPk.substring(1, splitPk.length() - 1).toLowerCase();
                }
            default:
                throw new DataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type.");
        }
    }


    public String quoteColumnName(String columnName) {
        switch (this) {
            case MySql:
            case Oracle:
                return "`" + columnName.replace("`", "``") + "`";
            case SQLServer:
                return "[" + columnName + "]";
            default:
                throw new DataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type");
        }
    }

    public String quoteTableName(String tableName) {
        switch (this) {
            case MySql:
            case Oracle:
                return "`" + tableName.replace("`", "``") + "`";
            case SQLServer:
                return tableName;
            default:
                throw new DataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type");
        }
    }

}
