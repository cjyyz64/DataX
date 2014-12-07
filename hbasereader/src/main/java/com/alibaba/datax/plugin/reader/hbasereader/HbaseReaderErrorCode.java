package com.alibaba.datax.plugin.reader.hbasereader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum HbaseReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("HbaseReader-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("HbaseReader-01", "您配置的值不合法."),
    TEMP("HbaseReader-01", "temp error code."),

    ;

    private final String code;
    private final String description;

    private HbaseReaderErrorCode(String code, String description) {
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
