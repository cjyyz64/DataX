package com.alibaba.datax.plugin.writer.mysqlwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum MysqlWriterErrorCode implements ErrorCode {
    CONF_ERROR("MysqlWriter-00", "username/password not filled."),
    WRITE_DATA_ERROR("MysqlWriter-01", "write data failed."),
    EXECUTE_SQL_ERROR("MysqlWriter-02", "execute sql failed."),
    NOT_RECOMMENDED("MysqlReader-03", "your config not recommended."),
    SESSION_ERROR("MysqlReader-04", "deal session failed."),;

    private final String code;
    private final String describe;

    private MysqlWriterErrorCode(String code, String describe) {
        this.code = code;
        this.describe = describe;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.describe;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Describe:[%s]. ", this.code,
                this.describe);
    }
}
