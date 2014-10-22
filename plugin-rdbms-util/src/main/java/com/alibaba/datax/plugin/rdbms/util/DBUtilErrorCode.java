package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.spi.ErrorCode;

//TODO
public enum DBUtilErrorCode implements ErrorCode {
    CONN_DB_ERROR("DBUtilErrorCode-00", "Connect to DataBase failed."),
    JDBC_CONTAINS_BLANK_ERROR("DBUtilErrorCode-01", "Some blank in jdbcUrl."),
    UNSUPPORTED_TYPE("DBUtilErrorCode-02", "unsupported database type."),
    COLUMN_SPLIT_ERROR(
            "DBUtilErrorCode-10", "Column split failed ."),

    REQUIRED_KEY("DBUtilErrorCode-00", "lost necessary key"),
    REQUIRED_VALUE("DBUtilErrorCode-01", "lost necessary value"),
    ILLEGAL_KEY("DBUtilErrorCode-02", "illegal key"),
    ILLEGAL_VALUE("DBUtilErrorCode-03", "illegal value"),
    ILLEGAL_SPLIT_PK("DBUtilErrorCode-04", "illegal split pk value"),
    NOT_RECOMMENDED("DBUtilErrorCode-05", "your config not recommended"),
    SQL_EXECUTE_FAIL("DBUtilErrorCode-06", "failed to execute mysql sql"),
    READ_RECORD_FAIL("DBUtilErrorCode-07", "failed to read mysql record"),
    TABLE_QUERYSQL_MIXED("DBUtilErrorCode-08", "can not config both table and querySql"),
    TABLE_QUERYSQL_MISSING("DBUtilErrorCode-09", "table and querySql should configured one item."),;

    private final String code;

    private final String description;

    private DBUtilErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
