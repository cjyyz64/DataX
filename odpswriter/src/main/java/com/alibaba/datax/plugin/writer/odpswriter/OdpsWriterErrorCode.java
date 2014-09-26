package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum OdpsWriterErrorCode implements ErrorCode {
    REQUIRED_KEY("OdpsReader-00", "lost necessary key"),
    REQUIRED_VALUE("OdpsReader-01", "lost necessary value"),
    ILLEGAL_VALUE("OdpsReader-02", "illegal value"),

    RUNTIME_EXCEPTION("OdpsReader-03", "run time exception"),
    NOT_SUPPORT_TYPE("OdpsReader-04", "not supported column type");

    private final String code;
    private final String description;

    private OdpsWriterErrorCode(String code, String description) {
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
