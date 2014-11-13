package com.alibaba.datax.plugin.reader.drdsreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum DrdsReaderErrorCode implements ErrorCode {
    CONFIG_ERROR("DrdsReader-00", "您配置错误."),
    GET_TOPOLOGY_FAILED("DrdsReader-01", "获取 drds 表的拓扑结构失败."),;

    private final String code;
    private final String description;

    private DrdsReaderErrorCode(String code, String description) {
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
