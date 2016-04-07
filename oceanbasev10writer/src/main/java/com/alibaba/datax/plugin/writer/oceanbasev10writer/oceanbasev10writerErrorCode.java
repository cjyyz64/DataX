package com.alibaba.datax.plugin.writer.oceanbasev10writer;

import com.alibaba.datax.common.spi.ErrorCode;

public enum oceanbasev10writerErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("oceanbasev10writer-00", "您配置的值不合法."),;

    private final String code;
    private final String description;

    private oceanbasev10writerErrorCode(String code, String description) {
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