package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum OdpsReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("OdpsReader-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("OdpsReader-01", "您配置的值不合法."),
    CREATE_DOWNLOADSESSION_FAIL("OdpsReader-03", "创建 ODPS 的 downloadSession 失败."),
    GET_DOWNLOADSESSION_FAIL("OdpsReader-04", "获取 ODPS 的 downloadSession 失败."),
    READ_DATA_FAIL("OdpsReader-05", "读取 ODPS 源头表失败."),;

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
