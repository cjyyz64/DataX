package com.alibaba.datax.plugin.writer.mongodbwriter;

/**
 * Created by jianying.wcj on 2015/3/17 0017.
 */
public class KeyConstant {
    /**
     * mongodb 的 host 地址
     */
    public static final String MONGO_HOST = "mongo_host";
    /**
     * mongodb 的 端口号
     */
    public static final String MONGO_PORT = "mongo_port";
    /**
     * mongodb 的用户名
     */
    public static final String MONGO_USER_NAME = "mongo_user_name";
    /**
     * mongodb 密码
     */
    public static final String MONGO_USER_PASSWORD = "mongo_user_password";
    /**
     * 是否验证权限
     */
    public static final String MONGO_IS_AUTH = "mongo_user_auth";
    /**
     * mongodb 数据库名
     */
    public static final String MONGO_DB_NAME = "mongo_db_name";
    /**
     * mongodb 集合名
     */
    public static final String MONGO_COLLECTION_NAME = "mongo_collection_name";
    /**
     * 批处理的大小
     */
    public static final String BATCH_SIZE = "batch_size";
    /**
     * 是否包含数组类型
     */
    public static final String IS_CONTAIN_ARRAY = "is_contain_array";
    /**
     * 数组分隔符
     */
    public static final String ARRAY_SPLITTER = "array_splitter";
}
