package com.alibaba.datax.plugin.writer.hbasebulkwriter2.conf;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * Created by liqiang on 15/5/25.
 */
public class FixColumnConf implements HBaseJobParameterConf {
    public String hbase_table;
    public List<RowkeyColumn> hbase_rowkey = Lists.newArrayList();
    public List<HbaseColumn> hbase_column = Lists.newArrayList();;
    public String hbase_output;
    public String hbase_config;
    public String hbase_cluster_name;
    public String hbase_hmc_address;
    public String hdfs_config;
    public Map<String, String> optional;


    public static class HbaseColumn {
       public String index;
       public  String hname;
       public String htype;

        public String getIndex() {
            return index;
        }

        public String getHname() {
            return hname;
        }

        public String getHtype() {
            return htype;
        }
    }

    public static class RowkeyColumn {
        public String index;
        public String htype;
        public String constant;

        public String getIndex() {
            return index;
        }

        public String getHtype() {
            return htype;
        }

        public String getConstant() {
            return constant;
        }
    }

    public String getHbase_table() {
        return hbase_table;
    }

    public List<RowkeyColumn> getHbase_rowkey() {
        return hbase_rowkey;
    }

    public List<HbaseColumn> getHbase_column() {
        return hbase_column;
    }

    public String getHbase_output() {
        return hbase_output;
    }

    public String getHbase_config() {
        return hbase_config;
    }

    public String getHdfs_config() {
        return hdfs_config;
    }

    public String getHbase_cluster_name() {
        return hbase_cluster_name;
    }

    public Map<String, String> getOptional() {
        return optional;
    }

    public String getHbase_hmc_address() {
        return hbase_hmc_address;
    }
}
