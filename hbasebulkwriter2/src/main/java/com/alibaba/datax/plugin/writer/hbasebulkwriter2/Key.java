package com.alibaba.datax.plugin.writer.hbasebulkwriter2;

public final class Key {

    /**
     * 此处声明插件用到的需要插件使用者提供的配置项
     */
    public final static String USERNAME = "username";

    public final static String READER = "job.content[0].reader";

    public final static String WRITER = "job.content[0].writer";

    public static final String READER_PARAMETER = READER + ".parameter";

    public static final String WRITER_PARAMETER = WRITER + ".parameter";

    public static final String PARAMETER_TYPE_ORIGIN = "origin";

    public static final String PARAMETER_TYPE_DYNAMICCOLUMN = "dynamiccolumn";

    public static final String PARAMETER_TYPE_FIXEDCOLUMN = "fixedcolumn";


    public static final String KEY_PROJECT = "project";

    public static final String KEY_TABLE = "table";

    public static final String KEY_ACCESSID = "accessId";

    public static final String KEY_ACCESSKEY = "accessKey";

    public static final String KEY_ODPSSERVER = "odpsServer";

    public static final String KEY_TUNNELSERVER = "tunnelServer";

    public static final String KEY_COLUMN = "column";

    public static final String KEY_PARTITION = "partition";

    public static final String KEY_SPLITMODE = "splitMode";

    public static final String KEY_HBASE_TABLE = "hbase_table";

    public static final String KEY_HBASE_ROWKEY = "hbase_rowkey";

    public static final String KEY_ROWKEY_TYPE = "rowkey_type";

    public static final String KEY_HBASE_COLUMN = "hbase_column";

    public static final String KEY_HBASE_OUTPUT = "hbase_output";

    public static final String KEY_HBASE_CONFIG = "hbase_config";

    public static final String KEY_CLUSTERID = "clusterId";

    public static final String KEY_BUCKETNUM = "bucketNum";

    public static final String KEY_HDFS_CONFIG = "hdfs_config";

    public static final String KEY_OPTIONAL = "optional";


}
