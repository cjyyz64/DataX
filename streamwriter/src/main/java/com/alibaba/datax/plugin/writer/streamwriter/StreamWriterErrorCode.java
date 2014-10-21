package com.alibaba.datax.plugin.writer.streamwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum StreamWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("StreamWriter-00", "lost necessary value"),
    ILLEGAL_VALUE("StreamWriter-01", "illegal value"),
    NOT_SUPPORT_TYPE("StreamWriter-02", "not supported column type"),
    RUNTIME_EXCEPTION("StreamWriter-03", "runtime exception"),;


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
