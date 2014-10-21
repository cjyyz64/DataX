package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum OdpsWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("OdpsWriter-00", "lost necessary value"),
    ILLEGAL_VALUE("OdpsWriter-01", "illegal value"),
    CONFIG_INNER_ERROR("OdpsWriter-02", "inner config error(some config error only can be found after runtime)"),
    UNSUPPORTED_ACCOUNT_TYPE("OdpsWriter-03", "unsupported account type"),
    UNSUPPORTED_COLUMN_TYPE("OdpsWriter-04", "unsupported column type"),

    TABLE_TRUNCATE_ERROR("OdpsWriter-05", "error while truncate table."),
    CREATE_MASTER_UPLOAD_FAIL("OdpsWriter-06", "failed to create master upload"),
    COLUMN_NUMBER_ERROR("OdpsWriter-07", "Column number not match."),
    GET_SLAVE_UPLOAD_FAIL("OdpsWriter-08", "failed to get slave upload"),
    GET_ID_KEY_FAIL("OdpsWriter-09", "failed to get accessId/accessKey"),

    GET_PARTITION_FAIL("OdpsWriter-10", "failed to get table all partitions"),
    GET_TABLE_DDL_FAIL("OdpsWriter-11", "failed to get table ddl sql"),

    WRITER_RECORD_FAIL("OdpsWriter-12", "failed to write odps record"),
    WRITER_BLOCK_FAIL("OdpsWriter-13", "failed to write odps block"),

    COMMIT_BLOCK_FAIL("OdpsWriter-14", "failed to commit block"),
    COLUMN_CONFIGURED_ERROR("OdpsWriter-15", "column configured error."),
    ADD_PARTITION_FAILED("OdpsWriter-16", "add partition failed."),
    CHECK_IF_PARTITIONED_TABLE_FAILED("OdpsWriter-17", "Check if partitioned table failed."),;

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
