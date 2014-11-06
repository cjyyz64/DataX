package com.alibaba.datax.plugin.reader.odpsreader;

public class Constant {

    public final static String START_INDEX = "startIndex";

    public final static String STEP_COUNT = "stepCount";

    public final static String SESSION_ID = "sessionId";

    public final static String IS_PARTITIONED_TABLE = "isPartitionedTable";

    public static final String ALL_COLUMN_PARSED_WITH_CONSTANT = "allColumnParsedWithConstant";

    public static final String COLUMN_POSITION = "columnPosition";

    public static final String DEFAULT_ACCOUNT_TYPE = "aliyun";

    // 常量字段用COLUMN_CONSTANT_FLAG 首尾包住即可
    public final static String COLUMN_CONSTANT_FLAG = "'";

    public final static int DEFAULT_RETRY_TIME = 3;

    /**
     * 以下是获取accesskey id 需要用到的常量值
     */
    public static final String SKYNET_ACCESSID = "SKYNET_ACCESSID";

    public static final String SKYNET_ACCESSKEY = "SKYNET_ACCESSKEY";

}
