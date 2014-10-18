package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum OdpsWriterErrorCode implements ErrorCode {
    REQUIRED_KEY("OdpsWriter-00", "lost necessary key"),
    REQUIRED_VALUE("OdpsWriter-01", "lost necessary value"),
    ILLEGAL_VALUE("OdpsWriter-03", "illegal value"),

    CHECK_TABLE_FAIL("OdpsWriter-04", "check table fail."),
    UNSUPPORTED_ACCOUNT_TYPE("OdpsWriter-05", "unsupported account type"),
    UNSUPPORTED_COLUMN_TYPE("OdpsWriter-06", "unsupported column type"),

    UNSUPPORTED_TABLE_TYPE("OdpsWriterXXXXXXXX", "unsupported table type"),

    CREATE_MASTER_UPLOAD_FAIL("OdpsWriter-07", "failed to create master upload"),
    GET_SLAVE_UPLOAD_FAIL("OdpsWriter-08", "failed to get slave upload"),
    GET_SESSION_STATUS_FAIL("OdpsWriter-09", "failed to get session status"),

    GET_PARTITION_FAIL("OdpsWriter-10", "failed to get table all partitions"),

    DELETE_PARTITION_FAIL("OdpsWriter-11", "failed to delete partition"),
    CREATE_PARTITION_FAIL("OdpsWriter-12", "failed to create partition"),
    TRUNCATE_TABLE_FAIL("OdpsWriter-13", "failed to truncate table"),
    WRITER_RECORD_FAIL("OdpsWriter-14", "failed to write odps record"),
    WRITER_BLOCK_FAIL("OdpsWriter-15", "failed to write odps block"),

    COMMIT_BLOCK_FAIL("OdpsWriter-16", "failed to commit block"),

    TEMP("TEMP", "Todo"),;


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
