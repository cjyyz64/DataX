package com.alibaba.datax.plugin.writer.adswriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum AdsWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("AdsWriter-00", "您缺失了必须填写的参数值."),
    ODPS_CREATETABLE_FAILED("AdsWriter-02", "创建ODPS临时表失败，请联系ADS 技术支持"),
    ADS_LOAD_DATA_FAILED("AdsWriter-03", "ADS从ODPS临时表倒数据失败，请联系ADS 技术支持"),
    TABLE_TRUNCATE_ERROR("AdsWriter-04", "清空 ODPS 目的表时出错."),
    Create_ADSHelper_FAILED("AdsWriter-05", "创建ADSHelper对象出错，请联系ADS 技术支持"),;

    private final String code;
    private final String description;


    private AdsWriterErrorCode(String code, String description) {
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
