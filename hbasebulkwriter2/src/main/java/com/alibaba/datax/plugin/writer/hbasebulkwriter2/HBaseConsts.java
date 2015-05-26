package com.alibaba.datax.plugin.writer.hbasebulkwriter2;

import org.apache.hadoop.hbase.HConstants;

public class HBaseConsts {

  /**
   * The home of plugin
   */
  public static final String PLUGIN_HOME = System.getProperty("datax.home","/home/admin/datax3") + "/plugin/writer/hbasebulkwriter2";

  /**
   * Default values of plugin params
   */
  public static final String PHOENIX_STYLE_DEFAULT = "false";
  public static final String BUCKET_NUM_DEFAULT = "-1";
  public static final boolean DYNAMIC_QUALIFIER_DEFAULT = false;
  public static final boolean TRUNCATE_DEFAULT = false;
  public static final String TIME_COL_DEFAULT = "-1";
  public static final String START_TS_DEFAULT = "-1";
  public static final String NULL_MODE_EMPTY_BYTES = "EMPTY_BYTES";
  public static final String NULL_MODE_SKIP = "SKIP";
  public static final String NULL_MODE_DELETE = "DELETE";
  public static final String NULL_MODE_DEFAULT = NULL_MODE_EMPTY_BYTES;

  public static final String[] HBASE_CONF_KEYS = {
          HConstants.ZOOKEEPER_QUORUM,
          HConstants.ZOOKEEPER_CLIENT_PORT,
          HConstants.ZOOKEEPER_ZNODE_PARENT
  };

  /**
   * Diamond data id
   */
  public static final String DIAMOND_DATA_ID_HBASE_CONF = "com.alibaba.hbase.cluster.%s.hbase.xml";
  public static final String DIAMOND_DATA_ID_HDFS_CONF = "com.alibaba.hbase.cluster.%s.hdfs.xml";
  public static final String DIAMOND_GROUP = "conf";
  public static final int DIAMOND_TIMEOUT = 10000;

}
