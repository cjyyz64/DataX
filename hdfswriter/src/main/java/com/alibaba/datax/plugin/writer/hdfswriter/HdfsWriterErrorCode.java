package com.alibaba.datax.plugin.writer.hdfswriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum HdfsWriterErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("HdfsWriter-00", "您配置的值不合法."),;

    private final String code;
    private final String description;

    private HdfsWriterErrorCode(String code, String description) {
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