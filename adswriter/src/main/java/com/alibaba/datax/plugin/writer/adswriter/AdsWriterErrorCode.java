package com.alibaba.datax.plugin.writer.adswriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum AdsWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("AdsWriter-00", "您缺失了必须填写的参数值."),
    ADS_TABLEINFO_FAILED("AdsWriter-01", "ADS GetTableInfo 失败，请联系ADS 技术支持"),
    ODPS_CREATETABLE_FAILED("AdsWriter-02", "创建ODPS临时表失败，请联系ADS 技术支持"),
    ADS_LOAD_DATA_FAILED("AdsWriter-03", "ADS从ODPS临时表倒数据失败，请联系ADS 技术支持"),

    ILLEGAL_VALUE("AdsWriter-0", "您配置的值不合法."),
    UNSUPPORTED_COLUMN_TYPE("AdsWriter-0", "DataX 不支持写入 ODPS 的目的表的此种数据类型."),

    TABLE_TRUNCATE_ERROR("AdsWriter-0", "清空 ODPS 目的表时出错."),
    CREATE_MASTER_UPLOAD_FAIL("AdsWriter-0", "创建 ODPS 的 uploadSession 失败."),
    GET_SLAVE_UPLOAD_FAIL("AdsWriter-0", "获取 ODPS 的 uploadSession 失败."),
    GET_ID_KEY_FAIL("AdsWriter-0", "获取 accessId/accessKey 失败."),
    GET_PARTITION_FAIL("AdsWriter-0", "获取 ODPS 目的表的所有分区失败."),

    ADD_PARTITION_FAILED("AdsWriter-0", "添加分区到 ODPS 目的表失败."),
    WRITER_RECORD_FAIL("AdsWriter-0", "写入数据到 ODPS 目的表失败."),

    COMMIT_BLOCK_FAIL("AdsWriter-1", "提交 block 到 ODPS 目的表失败."),
    RUN_SQL_FAILED("AdsWriter-1", "执行 ODPS Sql 失败."),
    CHECK_IF_PARTITIONED_TABLE_FAILED("AdsWriter-1", "检查 ODPS 目的表:%s 是否为分区表失败."),;

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
