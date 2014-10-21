package com.alibaba.datax.plugin.reader.streamreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum StreamReaderErrorCode implements ErrorCode {
    NOT_SUPPORT_TYPE("StreamReader-01", "not supported column type"),
    REQUIRED_VALUE("StreamReader-02", "lost necessary value"),
    ILLEGAL_VALUE("StreamReader-03", "illegal value"),;

    private final String code;
    private final String description;

    private StreamReaderErrorCode(String code, String description) {
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
