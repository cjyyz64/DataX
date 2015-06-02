package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.spi.ErrorCode;

//TODO
public enum DBUtilErrorCode implements ErrorCode {
    //连接错误
    MYSQL_CONN_USERPWD_ERROR("MYSQLErrCode-01","用户名或者密码错误，连接失败，请联系DBA确认"),
    MYSQL_CONN_IPPORT_ERROR("MYSQLErrCode-02","IP地址或者Port错误，请联系DBA确认"),
    MYSQL_CONN_DB_ERROR("MYSQLErrCode-03","数据库不存在，请联系DBA确认"),

    ORACLE_CONN_USERPWD_ERROR("ORACLEErrCode-01","用户名或者密码错误，连接失败，请联系DBA确认"),
    ORACLE_CONN_IPPORT_ERROR("ORACLEErrCode-02","IP地址或者Port错误，请联系DBA确认"),
    ORACLE_CONN_DB_ERROR("ORACLEErrCode-03","数据库不存在，请联系DBA确认"),

    CONF_ERROR("DBUtilErrorCode-00", "您的配置错误."),
    CONN_DB_ERROR("DBUtilErrorCode-10", "连接数据库失败. 请检查您的 账号、密码、数据库名称、IP、Port或者向 DBA 寻求帮助(注意网络环境)."),
    GET_COLUMN_INFO_FAILED("DBUtilErrorCode-01", "获取表字段相关信息失败."),
    UNSUPPORTED_TYPE("DBUtilErrorCode-12", "不支持的数据库类型. 请注意查看 DataX 已经支持的数据库类型以及数据库版本."),
    COLUMN_SPLIT_ERROR("DBUtilErrorCode-13", "根据主键进行切分失败."),
    SET_SESSION_ERROR("DBUtilErrorCode-14", "设置 session 失败."),

    REQUIRED_VALUE("DBUtilErrorCode-03", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("DBUtilErrorCode-02", "您填写的参数值不合法."),
    ILLEGAL_SPLIT_PK("DBUtilErrorCode-04", "您填写的主键列不合法, DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型."),
    SPLIT_FAILED_ILLEGAL_SQL("DBUtilErrorCode-15", "DataX尝试切分表时, 执行数据库 Sql 失败. 请检查您的配置 table/splitPk/where 并作出修改."),
    SQL_EXECUTE_FAIL("DBUtilErrorCode-06", "执行数据库 Sql 失败, 请检查您的配置的 column/table/where/querySql或者向 DBA 寻求帮助."),

    // only for reader
    READ_RECORD_FAIL("DBUtilErrorCode-07", "读取数据库数据失败. 请检查您的配置的 column/table/where/querySql或者向 DBA 寻求帮助."),
    TABLE_QUERYSQL_MIXED("DBUtilErrorCode-08", "您配置凌乱了. 不能同时既配置table又配置querySql"),
    TABLE_QUERYSQL_MISSING("DBUtilErrorCode-09", "您配置错误. table和querySql 应该并且只能配置一个."),

    // only for writer
    WRITE_DATA_ERROR("DBUtilErrorCode-05", "往您配置的写入表中写入数据时失败."),
    NO_INSERT_PRIVILEGE("DBUtilErrorCode-11","数据库没有写权限，请联系DBA"),;

    private final String code;

    private final String description;

    private DBUtilErrorCode(String code, String description) {
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
