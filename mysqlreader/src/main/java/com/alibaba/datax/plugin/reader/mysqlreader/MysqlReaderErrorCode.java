package com.alibaba.datax.plugin.reader.mysqlreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum MysqlReaderErrorCode implements ErrorCode {
    REQUIRED_KEY("MysqlReader-00", "lost necessary key"),
    REQUIRED_VALUE("MysqlReader-01", "lost necessary value"),
    ILLEGAL_KEY("MysqlReader-02", "illegal key"),
    ILLEGAL_VALUE("MysqlReader-03", "illegal value"),
    ILLEGAL_SPLIT_PK("MysqlReader-04", "illegal split pk value"),
    NOT_RECOMMENDED("MysqlReader-05", "your config not recommended"),
    SQL_EXECUTE_FAIL("MysqlReader-06", "failed to execute mysql sql"),
    READ_RECORD_FAIL("MysqlReader-07", "failed to read mysql record"),
    TABLE_QUERYSQL_MIXED("MysqlReader-08", "can not config both table and querySql"),
    TABLE_QUERYSQL_MISSING("MysqlReader-09", "table and querySql should configured one item."),;

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
