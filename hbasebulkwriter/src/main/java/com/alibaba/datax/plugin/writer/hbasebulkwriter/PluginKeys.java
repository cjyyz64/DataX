/**
 *  (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.datax.plugin.writer.hbasebulkwriter;

public final class PluginKeys {
  
  public final static String PREFIX_FIXED = "fixedcolumn";
  public final static String PREFIX_DYNAMIC = "dynamiccolumn";

  /**
   * the path of hbase-site.xml
   */
  public final static String HBASE_CONFIG = "hbase_config";

  /**
   * the path of hdfs-site.xml
   */
  public final static String HDFS_CONFIG = "hdfs_config";

  /**
   * the name of hbase table
   */
  public final static String TABLE = "hbase_table";

  /**
   * the output dir of hfiles
   */
  public final static String OUTPUT = "hbase_output";

  /**
   * the fields which would be combined to a rowkey For Example:
   * 0|string,1|long
   */
  public final static String ROWKEY = "hbase_rowkey";

  /**
   * the columns to write into table For Example:
   * 0|long|family1:qualifier1,1|string
   * |family1:qualifier2,2|int|family2:qualifier1
   */
  public final static String COLUMN = "hbase_column";

  /**
   * type of rowkey
   */
  public final static String ROWKEY_TYPE = "rowkey_type";

  /**
   * cluster id
   */
  public final static String CLUSTER_ID = "cluster_id";

  /**
   * encoding
   */
  public final static String ENCODING = "optional.encoding";

  /**
   * timestamp column
   */
  public final static String TIME_COL = "optional.time_col";

  /**
   * start timestamp
   */
  public final static String START_TS = "optional.start_ts";
  
  /**
   * semantic of null, only effective when DYNAMIC_QUALIFIER = false
   * have three option: EMPTY_BYTES,SKIP,DELETE. default is EMPTY_BYTES
   */
  public final static String NULL_MODE = "optional.null_mode";
  
  /**
   * the number of bucket in phoenix style.
   * Default value: -1
   */
  public final static String BUCKET_NUM = "optional.bucket_num";

  /**
   * whether truncate table or not.
   */
  public final static String TRUNCATE = "optional.truncate_table";
}
