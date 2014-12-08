package com.alibaba.datax.plugin.reader.ossreader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by mengxin.liumx on 2014/12/7.
 */
public enum OssReaderErrorCode implements ErrorCode {
    // TODO: 修改错误码类型
    RUNTIME_EXCEPTION("OssReader-00", "运行时异常"), FILE_EXCEPTION(
            "OssFileReader-01", "文件读取异常"), CONFIG_INVALID_EXCEPTION(
            "OssFileReader-02", "参数配置错误"), NOT_SUPPORT_TYPE("OssReader-03",
            "不支持的类型"), CAST_VALUE_TYPE_ERROR("OssFileReader-04", "无法完成指定类型的转换"), SECURITY_EXCEPTION(
            "OssReader-05", "缺少权限"), ;

    private final String code;
    private final String description;

    private OssReaderErrorCode(String code, String description) {
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
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}