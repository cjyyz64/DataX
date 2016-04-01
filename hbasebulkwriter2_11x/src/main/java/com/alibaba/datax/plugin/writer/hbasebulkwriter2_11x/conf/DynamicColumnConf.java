package com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x.conf;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * Created by liqiang on 15/5/25.
 */
public class DynamicColumnConf implements HBaseJobParameterConf {
    public String hbaseTable;
    public String rowkeyType;
    public HbaseColumn hbaseColumn = new HbaseColumn();
    public String hbaseOutput;
    public String hbaseConfig;
    public String hdfsConfig;
    public String nullMode;
    public String startTs;
    public String timeCol;
    public String bucketNum;
    public String encoding;
    public Map<String, String> configuration;

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

    public String getHbaseTable() {
        return hbaseTable;
    }

    public String getRowkeyType() {
        return rowkeyType;
    }

    public HbaseColumn getHbaseColumn() {
        return hbaseColumn;
    }

    public String getHbaseOutput() {
        return hbaseOutput;
    }

    public String getHbaseConfig() {
        return hbaseConfig;
    }

    public String getHdfsConfig() {
        return hdfsConfig;
    }

    public String getNullMode() {
        return nullMode;
    }

    public String getStartTs() {
        return startTs;
    }

    public String getTimeCol() {
        return timeCol;
    }

    public String getBucketNum() {
        return bucketNum;
    }

    public String getEncoding() {
        return encoding;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }
}
