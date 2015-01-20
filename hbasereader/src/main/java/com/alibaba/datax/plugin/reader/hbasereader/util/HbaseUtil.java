package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.*;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class HbaseUtil {
    private static Logger LOG = LoggerFactory.getLogger(HbaseUtil.class);

    public static void doPretreatment(Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.HBASE_CONFIG,
                HbaseReaderErrorCode.TEMP);

        String mode = HbaseUtil.dealMode(originalConfig);
        originalConfig.set(Key.MODE, mode);

        originalConfig.getNecessaryValue(Key.TABLE, HbaseReaderErrorCode.TEMP);
        List<Map> column = originalConfig.getList(Key.COLUMN, Map.class);

        if (column == null) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "必须配置 Hbasereader 的 column 配置项.");
        }

        HbaseUtil.checkColumn(column);

        String encoding = originalConfig.getString(Key.ENCODING, "utf-8");
        originalConfig.set(Key.ENCODING, encoding);

        Boolean isBinaryRowkey = originalConfig.getBool(Key.IS_BINARY_ROWKEY);
        if (isBinaryRowkey == null) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, "Hbasereader 中必须配置 isBinaryRowkey 项，用于指定主键自身是否为二进制结构。isBinaryRowkey 本项可以配置为 true 或者 false. 分别对应于 DataX 内部调用Bytes.toBytesBinary(String rowKey) 或者Bytes.toBytes(String rowKey) 两个不同的 API.");
        }

        String startRowkey = originalConfig.getString(Constant.RANGE + "." + Key.START_ROWKEY);
        if (startRowkey != null) {
            originalConfig.set(Key.START_ROWKEY, startRowkey);
        }


        String endRowkey = originalConfig.getString(Constant.RANGE + "." + Key.END_ROWKEY);
        if (endRowkey != null) {
            originalConfig.set(Key.END_ROWKEY, endRowkey);
        }
    }

    private static String dealMode(Configuration originalConfig) {
        String mode = originalConfig.getString(Key.MODE, "normal");
        if (!mode.equalsIgnoreCase("normal") && !mode.equalsIgnoreCase("multiVersion")) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP,
                    "mode 仅能配置为 normal 或者 multiVersion .");
        }

        return mode;
    }

    private static void checkColumn(List<Map> column) {
        HbaseReader.parseColumn(column);
    }


    public static org.apache.hadoop.conf.Configuration getHbaseConf(String hbaseConf) {
        if (StringUtils.isBlank(hbaseConf)) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, "hbaseConf is blank !");
        }
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        Map<String, String> map = null;
        try {
            map = JSON.parseObject(hbaseConf, map.getClass());
        } catch (Exception e) {
            // 用户配置的hbase配置文件路径
            LOG.info("hbaseConf is File Path [{}].", hbaseConf);
            conf.addResource(new Path(hbaseConf));
            return conf;
        }

        // / 用户配置的key-value对来表示hbaseConf
        LOG.info("hbaseConf is JSON String [{}].", hbaseConf);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    public static byte[] getStartRowKey(Configuration configuration) {
        String startRowkey = configuration.getString(Key.START_ROWKEY);
        boolean isBinaryRowkey = configuration.getBool(Key.IS_BINARY_ROWKEY);

        return parseRowKeyByte(startRowkey, isBinaryRowkey);
    }

    public static byte[] getEndRowKey(Configuration configuration) {
        String endRowkey = configuration.getString(Key.END_ROWKEY);
        boolean isBinaryRowkey = configuration.getBool(Key.IS_BINARY_ROWKEY);

        return parseRowKeyByte(endRowkey, isBinaryRowkey);
    }


    public static HTable initHtable(com.alibaba.datax.common.util.Configuration configuration) {
        String hbaseConnConf = configuration.getString(Key.HBASE_CONFIG);
        String tableName = configuration.getString(Key.TABLE);
        try {
            org.apache.hadoop.conf.Configuration conf = HbaseUtil.getHbaseConf(hbaseConnConf);
            HBaseAdmin admin = HTableFactory.createHBaseAdmin(conf);
            HTable htable = HTableFactory.createHTable(conf, tableName);

            check(admin, htable);

            return htable;
        } catch (Exception e) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, e);
        }
    }


    private static void check(HBaseAdmin admin, HTable htable) throws DataXException, IOException {
        if (!admin.isMasterRunning()) {
            throw new IllegalStateException("HBase master is not running!");
        }
        if (!admin.tableExists(htable.getTableName())) {
            throw new IllegalStateException("HBase table " + Bytes.toString(htable.getTableName())
                    + " is not existed!");
        }
        if (!admin.isTableAvailable(htable.getTableName())) {
            throw new IllegalStateException("HBase table " + Bytes.toString(htable.getTableName())
                    + " is not available!");
        }
        if (!admin.isTableEnabled(htable.getTableName())) {
            throw new IllegalStateException("HBase table " + Bytes.toString(htable.getTableName())
                    + " is disable!");
        }
    }
    private static byte[] parseRowKeyByte(String rowkey, boolean isBinaryRowkey) {
        byte[] retRowKey;
        if (org.apache.commons.lang.StringUtils.isBlank(rowkey)) {
            retRowKey = HConstants.EMPTY_BYTE_ARRAY;
        } else {
            if (isBinaryRowkey) {
                retRowKey = Bytes.toBytesBinary(rowkey);
            } else {
                retRowKey = Bytes.toBytes(rowkey);
            }
        }
        return retRowKey;
    }
}
