package com.alibaba.datax.plugin.writer.ocswriter.utils;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Time:    2015-05-22 16:45
 * Creator: yuanqi@alibaba-inc.com
 */
public enum OcsWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("OcsWriterErrorCode-000", "参数不能为空"),
    ILLEGAL_PARAM_VALUE("OcsWriterErrorCode-001", "参数不合法"),
    HOST_UNREACHABLE("OcsWriterErrorCode-003", "服务不可用"),
    OCS_INIT_ERROR("OcsWriterErrorCode-004", "初始化ocs client失败"),
    DIRTY_RECORD("OcsWriterErrorCode-005", "脏数据"),
    COMMIT_FAILED("OcsWriterErrorCode-006", "提交数据到ocs失败");

    private final String code;
    private final String description;

    private OcsWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }
}
