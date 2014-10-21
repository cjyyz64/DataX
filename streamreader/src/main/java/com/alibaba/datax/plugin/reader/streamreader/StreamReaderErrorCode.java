package com.alibaba.datax.plugin.reader.streamreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum StreamReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("StreamReader-00", "lost necessary value"),
    ILLEGAL_VALUE("StreamReader-01", "illegal value"),
    NOT_SUPPORT_TYPE("StreamReader-02", "not supported column type"),;


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
