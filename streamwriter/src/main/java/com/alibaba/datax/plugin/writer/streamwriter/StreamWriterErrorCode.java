package com.alibaba.datax.plugin.writer.streamwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum StreamWriterErrorCode implements ErrorCode {
    RUNTIME_EXCEPTION("StreamWriter-00", "运行时异常"),;


    private final String code;
    private final String description;

    private StreamWriterErrorCode(String code, String description) {
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
