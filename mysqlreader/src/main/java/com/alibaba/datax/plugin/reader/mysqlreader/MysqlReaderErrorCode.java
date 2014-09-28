package com.alibaba.datax.plugin.reader.mysqlreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum MysqlReaderErrorCode implements ErrorCode {
    REQUIRED_KEY("MysqlReader-00", "lost necessary key"),
    REQUIRED_VALUE("MysqlReader-01", "lost necessary value"),
    ILLEGAL_KEY("MysqlReader-02", "illegal key"),
    ILLEGAL_VALUE("MysqlReader-03", "illegal value"),

    SQL_EXECUTE_FAIL("MysqlReader-13", "failed to execute mysql sql"),
    READ_RECORD_FAIL("MysqlReader-14", "failed to read mysql record"),


    CONF_ERROR("MysqlReader-00", "username/password not filled"),
    UNKNOWN_ERROR("MysqlReader-01", "todo"),
    NOT_RECOMMENDED("MysqlReader-02", "your config not recommended"),
    TABLE_QUERYSQL_MIXED("MysqlReader-03", "Can not config both table and querySql");

    private final String code;
    private final String description;

    private MysqlReaderErrorCode(String code, String description) {
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
