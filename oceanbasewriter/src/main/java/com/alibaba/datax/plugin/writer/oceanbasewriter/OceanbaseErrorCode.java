package com.alibaba.datax.plugin.writer.oceanbasewriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum OceanbaseErrorCode implements ErrorCode {
   DESC("Fetch column meta fail from OB","desc target table error");

    private final String code;
    private final String describe;

    private OceanbaseErrorCode(String code, String describe) {
        this.code = code;
        this.describe = describe;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.describe;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Describe:[%s]. ", this.code,
                this.describe);
    }
}
