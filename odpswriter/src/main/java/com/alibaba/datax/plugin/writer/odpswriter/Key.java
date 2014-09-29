package com.alibaba.datax.plugin.writer.odpswriter;

public class Key {

    public final static String ACCESS_ID = "accessId";

    public final static String ACCESS_KEY = "accessKey";

    public final static String ODPS_SERVER = "odpsServer";

    // 线上环境不需要填写，线下环境必填
    public final static String TUNNEL_SERVER = "tunnelServer";

    public static final String PROJECT = "project";

    public final static String TABLE = "table";

    //TODO 表示运行时所在的 project
    public final static String CURRENT_PROJECT = "currentProject";

    // 不同于odpsreader 这里的分区配置只能是单个字符串，不能是 List
    public final static String PARTITION = "partition";

    public final static String COLUMN = "column";

    //TODO 必填
    public final static String TRUNCATE = "truncate";

    public final static String MAX_RETRY_TIME = "maxRetryTime";

    // 账号类型，默认为aliyun，也可能为taobao等其他类型 domainType（TODO 需要其他配置项）
    public final static String ACCOUNT_TYPE = "accountType";

}
