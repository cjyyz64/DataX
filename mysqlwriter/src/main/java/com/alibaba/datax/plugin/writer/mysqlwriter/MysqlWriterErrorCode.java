package com.alibaba.datax.plugin.writer.mysqlwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum MysqlWriterErrorCode implements ErrorCode {
    CONF_ERROR("MysqlWriter-00", "您的配置错误."),
    REQUIRED_VALUE("MysqlWriter-00", "您缺失了必须填写的参数值."),
    WRITE_MODE_CONF_ERROR("MysqlWriter-00", "您所配置的写入目的表的写入模式(writeMode)错误."),
    BATCH_SIZE_CONF_ERROR("MysqlWriter-00", "您所配置的写入目的表的批量提交行数(batchSize)错误."),
    COLUMN_CONF_ERROR("MysqlWriter-00", "您所配置的写入目的表的列(column)错误."),
    WRITE_DATA_ERROR("MysqlWriter-01", "往您配置的写入表中写入数据时失败."),
    UNSUPPORTED_DATA_TYPE("MysqlWriter-02", "不支持此种数据类型."),
    EXECUTE_SQL_ERROR("MysqlWriter-03", "执行 Sql 语句错误."),
    SESSION_ERROR("MysqlReader-05", "处理 session 配置时出错."),;

    private final String code;
    private final String describe;

    private MysqlWriterErrorCode(String code, String describe) {
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
