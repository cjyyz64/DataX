package com.alibaba.datax.plugin.rdbms.writer;

public final class Key {
    public final static String JDBC_URL = "jdbcUrl";

    public final static String USERNAME = "username";

    public final static String PASSWORD = "password";

    public final static String TABLE = "table";

    public final static String COLUMN = "column";

    //可选值为：insert,replace，默认为 insert （mysql 支持，oracle 没用 replace 机制，只能 insert,oracle 可以不暴露这个参数）
    public final static String WRITE_MODE = "writeMode";

    public final static String PRE_SQL = "preSql";

    public final static String POST_SQL = "postSql";

    public final static String TDDL_APP_NAME = "tddlAppName";

    //默认值：256
    public final static String BATCH_SIZE = "batchSize";

    //TODO delete it
    public final static String SESSION = "session";

}