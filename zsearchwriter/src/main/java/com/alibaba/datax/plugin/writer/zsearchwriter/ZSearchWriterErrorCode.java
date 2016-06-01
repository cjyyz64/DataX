package com.alibaba.datax.plugin.writer.zsearchwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum ZSearchWriterErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("ZSearchWriter-00", "您配置的值不合法."),
    BAD_WRITE_VALUE("ZSearchWriter-01", "数据写入出错."),;

    private final String code;
    private final String description;

    private ZSearchWriterErrorCode(String code, String description) {
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