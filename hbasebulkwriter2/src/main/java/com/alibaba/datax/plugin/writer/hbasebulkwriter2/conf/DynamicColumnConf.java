package com.alibaba.datax.plugin.writer.hbasebulkwriter2.conf;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * Created by liqiang on 15/5/25.
 */
public class DynamicColumnConf implements HBaseJobParameterConf {
    public String hbase_table;
    public String rowkey_type;
    public HbaseColumn hbase_column = new HbaseColumn();
    public String hbase_output;
    public String hbase_cluster_name;
    public String hbase_hmc_address;
    public String hbase_config;
    public String hdfs_config;
    public Map<String, String> optional;

    public static class HbaseColumn {
        public String type = "prefix";
        public List<HBaseRule> rules = Lists.newArrayList();

        public String getType() {
            return type;
        }

        public List<HBaseRule> getRules() {
            return rules;
        }
    }

    public static class HBaseRule {
        public String pattern;
        public String htype;

        public String getPattern() {
            return pattern;
        }

        public String getHtype() {
            return htype;
        }
    }

    public String getHbase_table() {
        return hbase_table;
    }

    public String getRowkey_type() {
        return rowkey_type;
    }

    public HbaseColumn getHbase_column() {
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

    public String getHbase_hmc_address() {
        return hbase_hmc_address;
    }

    public Map<String, String> getOptional() {
        return optional;
    }
}
