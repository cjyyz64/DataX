package com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x.conf;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * Created by liqiang on 15/5/25.
 */
public class FixColumnConf implements HBaseJobParameterConf {
    public String hbaseTable;
    public List<RowkeyColumn> hbaseRowkey = Lists.newArrayList();
    public List<HbaseColumn> hbaseColumn = Lists.newArrayList();;
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

    public String getHbaseTable() {
        return hbaseTable;
    }

    public List<RowkeyColumn> getHbaseRowkey() {
        return hbaseRowkey;
    }

    public List<HbaseColumn> getHbaseColumn() {
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
