package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.spi.ErrorCode;

//TODO
public enum DBUtilErrorCode implements ErrorCode {
    CONN_DB_ERROR("DBUtilErrorCode-00", "Connect to DataBase failed: Please check your IP/Port/Database/User/Pass is available !"),
    JDBC_CONTAINS_BLANK_ERROR("DBUtilErrorCode-01", "Configuration jdbcUrl Cannot be null :  Please check your configuration of jdbcUrl !"),
    UNSUPPORTED_TYPE("DBUtilErrorCode-02", "Unsupported database type: I cannot help you, sorry. Please ask your administrator for HELP !"),
    COLUMN_SPLIT_ERROR("DBUtilErrorCode-10", "Column split failed ."),

    REQUIRED_KEY("DBUtilErrorCode-00", "Lost required key: Please make key required available !"),
    REQUIRED_VALUE("DBUtilErrorCode-01", "Lost necessary value:  Please make value required available !"),
    ILLEGAL_KEY("DBUtilErrorCode-02", "Illegal key: Please correct your key !"),
    ILLEGAL_VALUE("DBUtilErrorCode-03", "Illegal value: Please correct your value !"),
    ILLEGAL_SPLIT_PK("DBUtilErrorCode-04", "Illegal splitPk value: SplitPk must be PrimaryKey in table and column type must be integer or string ."),
    NOT_RECOMMENDED("DBUtilErrorCode-05", "Your config not recommended: Please read DataX manual and modify your configuration ."),
    SQL_EXECUTE_FAIL("DBUtilErrorCode-06", "Failed to execute mysql sql: Please check your Column/Table/Where/querySql !"),
    READ_RECORD_FAIL("DBUtilErrorCode-07", "Failed to read mysql record: Please check your Column/Table/Where/querySql !"),
    TABLE_QUERYSQL_MIXED("DBUtilErrorCode-08", "Can not config both table and querySql: Please keep just ONE of them !"),
    TABLE_QUERYSQL_MISSING("DBUtilErrorCode-09", "Table and querySql should configured one item: Please keep ONE of them !"),;

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
