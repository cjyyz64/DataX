package com.alibaba.datax.plugin.reader.streamreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum StreamReaderErrorCode implements ErrorCode {
    RUNTIME_EXCEPTION("StreamReader-00", "run time exception"),
    NOT_SUPPORT_TYPE("StreamReader-01", "not supported column type"),
    REQUIRED_VALUE("StreamReader-01", "lost necessary value"),
    TEMP("StreamReader-01", "lost necessary value"),
    CAST_VALUE_TYPE_ERROR("StreamReader-02", "can not cast value to pointed type");

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
