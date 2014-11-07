package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum OdpsReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("OdpsWriter-00", "lost necessary value"),
    ILLEGAL_VALUE("OdpsWriter-01", "illegal value"),
    RUNTIME_EXCEPTION("OdpsReader-00", "run time exception"),
    NOT_SUPPORT_TYPE("OdpsReader-01", "not supported column type"),
    CREATE_DOWNLOADSESSION_FAIL("OdpsReader-02", "create downloadSession failed."),
    READ_DATA_FAIL("OdpsReader-03", "read data fail."),
    PARTITION_CONFIG_ERROR("OdpsReader-03", "partition configured error."),
    TABLE_NOT_EXIST("OdpsReader-03", "read data fail."),

    ;

    private final String code;
    private final String description;

    private OdpsReaderErrorCode(String code, String description) {
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
