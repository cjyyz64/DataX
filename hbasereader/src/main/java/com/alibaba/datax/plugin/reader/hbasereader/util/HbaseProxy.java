package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.hbasereader.*;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public class HbaseProxy {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseProxy.class);

    private static final int SCAN_CACHE = 256;

    private static final String META_SCANNER_CACHING = "100";

    private Configuration config;

    private HTable htable;
    private HBaseAdmin admin;
    private String encode = "UTF-8";
    private boolean needRowkey = false;
    private boolean isBinaryRowkey = false;
    private byte[] startKey = null;
    private byte[] endKey = null;
    private Scan scan = null;

    private byte[][] families = null;

    private byte[][] columns = null;

    private ResultScanner rs = null;

    private Result lastResult = null;

    public boolean isNeedRowkey() {
        return needRowkey;
    }

    public void setNeedRowkey(boolean needRowkey) {
        this.needRowkey = needRowkey;
    }

    public boolean isBinaryRowkey() {
        return isBinaryRowkey;
    }

    public void setBinaryRowkey(boolean isBinaryRowkey) {
        this.isBinaryRowkey = isBinaryRowkey;
    }

    public static HbaseProxy newProxy(String hbase_conf, String tableName) throws IOException {
        return new HbaseProxy(hbase_conf, tableName);
    }

    private HbaseProxy(String hbaseConf, String tableName) throws IOException {
        Configuration conf = getHbaseConf(hbaseConf);

        this.config = new Configuration(conf);
        this.config.set("hbase.meta.scanner.caching", META_SCANNER_CACHING);
        htable = com.alibaba.datax.plugin.reader.hbasereader.HTableFactory.createHTable(this.config, tableName);
        admin = com.alibaba.datax.plugin.reader.hbasereader.HTableFactory.createHBaseAdmin(this.config);

        if (!this.check()) {
            throw new IllegalStateException("DataX try to build HBaseProxy failed !");
        }
    }

    private Configuration getHbaseConf(String hbaseConf) {
        if (StringUtils.isBlank(hbaseConf)) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, "hbaseConf is blank !");
        }
        Configuration conf = new Configuration();

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
        for (Entry<String, String> entry : map.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
        return this.htable.getStartEndKeys();
    }

    public HTable getHtable() {
        return this.htable;
    }

    public void setEncode(String encode) {
        this.encode = encode;
    }

    public void setStartRange(byte[] startKey) {
        this.startKey = startKey;
    }

    public void setEndRange(byte[] endKey) {
        this.endKey = endKey;
    }

    public void setStartEndRange(byte[] startKey, byte[] endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
    }

    /*
     * Must be sure that column is in format like 'family: column'
     */
    public void prepare(String[] columns) throws IOException {
        this.scan = new Scan();
        this.scan.setCacheBlocks(false);

        if (this.startKey != null) {
            LOG.info(
                    "HBaseReader set startkey to {} .",
                    this.isBinaryRowkey() ? Bytes.toStringBinary(this.startKey) : Bytes
                            .toString(this.startKey));
            scan.setStartRow(startKey);
        }
        if (this.endKey != null) {
            LOG.info(
                    "HBaseReader set endkey to {} .",
                    this.isBinaryRowkey() ? Bytes.toStringBinary(this.endKey) : Bytes
                            .toString(this.endKey));
            scan.setStopRow(endKey);
        }

        this.families = new byte[columns.length][];
        this.columns = new byte[columns.length][];

        int idx = 0;
        for (String column : columns) {
            this.families[idx] = column.split(":")[0].trim().getBytes();
            this.columns[idx] = column.split(":")[1].trim().getBytes();
            scan.addColumn(this.families[idx], this.columns[idx]);
            idx++;
        }

        htable.setScannerCaching(SCAN_CACHE);
        this.rs = htable.getScanner(this.scan);
    }

    /**
     * @param columnType 只包含column的type，不包含rowkey的type，事实上，目前DataX中rowkey只支持string类型
     * @throws Exception
     */
    public boolean fetchLine(Record line, String[] columnType) throws Exception {
        if (null == this.rs) {
            throw new IllegalStateException("HBase Client try to fetch data failed .");
        }

        Result result;
        try {
            result = this.rs.next();
        } catch (IOException e) {
            if (this.lastResult != null) {
                this.scan.setStartRow(lastResult.getRow());
            }
            this.rs = htable.getScanner(this.scan);
            result = this.rs.next();
            if (this.lastResult != null && Bytes.equals(this.lastResult.getRow(), result.getRow())) {
                result = this.rs.next();
            }
        }
        if (null == result) {
            return false;
        }
        lastResult = result;

        try {
            // need to extract rowkey info
            if (this.isNeedRowkey()) {
                if (this.isBinaryRowkey()) {
                    line.addColumn(new StringColumn(Bytes.toStringBinary(result.getRow())));
                } else {
                    line.addColumn(new StringColumn(new String(result.getRow(), encode)));
                }
            }

            String tempValue = "";
            String tempValueType = "";
            for (int i = 0; i < this.families.length; i++) {
                byte[] value = result.getValue(this.families[i], this.columns[i]);
                if (null == value || 0 == value.length) {
                    //TODO
                    line.addColumn(null);
                } else {
                    tempValueType = columnType[i];
                    if (tempValueType.equals("string")) {
                        tempValue = new String(value, encode);
                    } else if (tempValueType.equals("int")) {
                        tempValue = String.valueOf(Bytes.toInt(value));
                    } else if (tempValueType.equals("long")) {
                        tempValue = String.valueOf(Bytes.toLong(value));
                    } else if (tempValueType.equals("short")) {
                        tempValue = String.valueOf(Bytes.toShort(value));
                    } else if (tempValueType.equals("float")) {
                        tempValue = String.valueOf(Bytes.toFloat(value));
                    } else if (tempValueType.equals("double")) {
                        tempValue = String.valueOf(Bytes.toDouble(value));
                    } else if (tempValueType.equals("boolean")) {
                        tempValue = String.valueOf(Bytes.toBoolean(value));
                    }

                    line.addColumn(new StringColumn(tempValue));
                }
            }
        } catch (Exception e) {
            // 注意，这里catch的异常，期望是byte数组转换失败的情况。而实际上，string的byte数组，转成整数类型是不容易报错的。但是转成double类型容易报错。
//            line.clear();

            if (this.isBinaryRowkey()) {
                line.addColumn(new StringColumn(Bytes.toStringBinary(result.getRow())));
            } else {
                line.addColumn(new StringColumn(new String(result.getRow(), encode)));
            }
            throw e;
        }

        return true;
    }

    public void close() throws IOException {
        if (null != rs) {
            rs.close();
        }
        if (null != htable) {
            htable.close();
            com.alibaba.datax.plugin.reader.hbasereader.HTableFactory.closeHtable();
        }
    }

    private boolean check() throws DataXException, IOException {
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

        return true;
    }

}
