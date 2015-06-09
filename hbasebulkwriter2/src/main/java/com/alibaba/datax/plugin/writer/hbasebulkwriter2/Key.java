package com.alibaba.datax.plugin.writer.hbasebulkwriter2;

public final class Key {

    /**
     * 此处声明插件用到的需要插件使用者提供的配置项
     */

    public final static String HDFS_DIR_BULKLOAD = "/group/datax3_hbasebulkwrite";

    public final static String ODPS_TMP_TBL_PREFIX = "t_dx3_o2h_tbl";

    public final static String READER = "job.content[0].reader";

    public final static String WRITER = "job.content[0].writer";

    public static final String READER_PARAMETER = READER + ".parameter";

    public static final String WRITER_PARAMETER = WRITER + ".parameter";

    public static final String PARAMETER_TYPE_ORIGIN = "origin";

    public static final String KEY_PROJECT = "project";

    public static final String KEY_TABLE = "table";

    public static final String KEY_ACCESSID = "accessId";

    public static final String KEY_ACCESSKEY = "accessKey";

    public static final String KEY_COLUMN = "column";

    public static final String KEY_PARTITION = "partition";

    public static final String KEY_HBASE_TABLE = "hbaseTable";

    public static final String KEY_HBASE_ROWKEY = "hbaseRowkey";

    public static final String KEY_ROWKEY_TYPE = "rowkeyType";

    /**
     * the columns to write into table For Example:
     * 0|long|family1:qualifier1,1|string
     * |family1:qualifier2,2|int|family2:qualifier1
     */
    public static final String KEY_HBASE_COLUMN = "hbaseColumn";

    //configuration是map形式的config内容的keyvalue，包含hbase、hadoop
    public static final String KEY_HBASE_CONFIGURATION = "configuration";

    public static final String KEY_HBASE_CONFIG = "hbaseConfig";

    public static final String KEY_HBASE_OUTPUT = "hbaseOutput";

    public static final String KEY_CLUSTERID = "clusterId";

    public static final String KEY_HDFS_CONFIG = "hdfsConfig";

    //public static final String KEY_OPTIONAL = "optional";

    public static final String KEY_OPTIONAL_BUCKETNUM = "bucketNum";

    public final static String KEY_ENCODING = "encoding";

    public final static String KEY_TIME_COL = "timeCol";

    /**
     * start timestamp
     */
    public final static String KEY_START_TS = "startTs";

    /**
     * semantic of null, only effective when DYNAMIC_QUALIFIER = false
     * have three option: EMPTY_BYTES,SKIP,DELETE. default is EMPTY_BYTES
     */
    public final static String KEY_NULL_MODE = "nullMode";

    /**
     * the number of bucket in phoenix style.
     * Default value: -1
     */
    public final static String KEY_BUCKET_NUM = "bucketNum";

    /**
     * whether truncate table or not.
     */
    public final static String KEY_TRUNCATE = "truncateTable";


    public final static String PREFIX_FIXED = "fixedcolumn";

    public final static String PREFIX_DYNAMIC = "dynamiccolumn";
}
