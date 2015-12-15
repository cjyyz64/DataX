package com.alibaba.datax.plugin.writer.hbasewriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by qifeng.sxm on 2015/12/11.
 */
public enum HBaseWriterErrorCode implements ErrorCode {

    ZIPS_CONFIG_ERROR("HBaseWriter-01", "没有配置 zips"),
    ZPORT_CONFIG_ERROR("HBaseWriter-02", "没有配置 zport"),
    TABLE_NAME_CONFIG_ERROR("HBaseWriter-03", "没有配置 table_name"),
    COLUMN_FAMILY_CONFIG_ERROR("HBaseWriter-04", "没有配置 column_family"),
    COLUMN_NAMES_CONFIG_ERROR("HBaseWriter-05", "没有配置 column_names"),
    ILLEGAL_VALUES_ERROR("HBaseWriter-06", "字段个数不一致");

    HBaseWriterErrorCode(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    private String code;
    private String desc;

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.desc;
    }
}
