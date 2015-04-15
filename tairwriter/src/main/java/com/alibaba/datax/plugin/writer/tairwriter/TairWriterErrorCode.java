package com.alibaba.datax.plugin.writer.tairwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum TairWriterErrorCode implements ErrorCode {

    TairInitError("TairWriter-01", "TairWriter参数配置错误"),
    TairDataError("TairWriter-02", "TairWriter数据错误"),
    TairRuntimeError("TairWriter-03", "TairWriter运行错误");

    private final String code;
    private final String description;

    private TairWriterErrorCode(String code, String description) {
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
