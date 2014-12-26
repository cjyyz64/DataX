package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.hbasereader.*;
import com.alibaba.datax.plugin.reader.hbasereader.HTableFactory;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.http.impl.cookie.DateUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class HbaseProxy {
    private final static Logger LOG = LogUtil.ReaderLog.getLogger(HbaseProxy.class);

    private static final String META_SCANNER_CACHING = "100";

    private Configuration config;
    private org.apache.commons.lang3.tuple.Pair<String, String> rangeInfo;

    private HTable htable;
    private HBaseAdmin admin;
    private String encoding;
    private boolean isBinaryRowkey;
    private byte[] startKey = null;
    private byte[] endKey = null;

    private Result lastResult = null;
    private Scan scan;
    private int scanCache;
    private ResultScanner resultScanner;

    public static HbaseProxy newProxy(com.alibaba.datax.common.util.Configuration configuration) {
        org.apache.commons.lang3.tuple.Pair<String, String> rangeInfo = HbaseUtil.dealRowkeyRange(configuration);

        return new HbaseProxy(configuration, rangeInfo);
    }

    private HbaseProxy(com.alibaba.datax.common.util.Configuration configuration,
                       org.apache.commons.lang3.tuple.Pair rangeInfo) {

        String userConfiguredHbaseSiteConfig = configuration.getString(Key.HBASE_CONFIG);
        String tableName = configuration.getString(Key.TABLE);
        this.encoding = configuration.getString(Key.ENCODING);
        this.isBinaryRowkey = configuration.getBool(Key.IS_BINARY_ROWKEY);

        this.scanCache = configuration.getInt(Key.SCAN_CACHE, Constant.DEFAULT_SCAN_CACHE);

        Configuration conf = getHbaseConf(userConfiguredHbaseSiteConfig);
        this.config = new Configuration(conf);

        this.config.set("hbase.meta.scanner.caching", META_SCANNER_CACHING);
        try {
            htable = HTableFactory.createHTable(this.config, tableName);
            admin = HTableFactory.createHBaseAdmin(this.config);

            this.check();
        } catch (Exception e) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, e);
        }

        this.rangeInfo = rangeInfo;
        dealRangeInfo(this.rangeInfo);
    }

    private void dealRangeInfo(org.apache.commons.lang3.tuple.Pair<String, String> rangeInfo) {
        String startRowkey = rangeInfo.getLeft();
        String endRowkey = rangeInfo.getRight();

        this.startKey = parseRowKeyByte(startRowkey, this.isBinaryRowkey);
        this.endKey = parseRowKeyByte(endRowkey, this.isBinaryRowkey);
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

    public void prepare(List<HbaseColumnCell> hbaseColumnCells) throws Exception {
        this.scan = new Scan();
        scan.setCacheBlocks(false);

        if (this.startKey != null) {
            LOG.info("HBaseReader set startkey to {} .", bytesToString(this.startKey, this.isBinaryRowkey, this.encoding));
            this.scan.setStartRow(startKey);
        }
        if (this.endKey != null) {
            LOG.info(
                    "HBaseReader set endkey to {} .", bytesToString(this.endKey, this.isBinaryRowkey, this.encoding));
            this.scan.setStopRow(endKey);
        }

        boolean isConstant;
        boolean isRowkeyColumn;
        for (HbaseColumnCell cell : hbaseColumnCells) {
            isConstant = cell.isConstant();
            isRowkeyColumn = this.isRowkeyColumn(cell.getColumnName());

            if (!isConstant && !isRowkeyColumn) {
                this.scan.addColumn(cell.getCf(), cell.getQualifier());
            }
        }

        this.scan.setCaching(this.scanCache);
        this.resultScanner = this.htable.getScanner(this.scan);
    }


    public boolean fetchLine(Record record, List<HbaseColumnCell> hbaseColumnCells) throws Exception {

        Result result;
        try {
            result = this.resultScanner.next();
        } catch (IOException e) {
            if (this.lastResult != null) {
                this.scan.setStartRow(this.lastResult.getRow());
            }
            this.resultScanner = htable.getScanner(scan);
            result = this.resultScanner.next();
            if (this.lastResult != null && Bytes.equals(this.lastResult.getRow(), result.getRow())) {
                result = this.resultScanner.next();
            }
        }
        if (null == result) {
            return false;
        }
        this.lastResult = result;

        try {
            byte[] tempValue;
            String columnName;
            ColumnType columnType;

            byte[] cf;
            byte[] qualifier;

            for (HbaseColumnCell cell : hbaseColumnCells) {
                columnType = cell.getColumnType();
                if (cell.isConstant()) {
                    // 对常量字段的处理
                    fillRecordWithConstantValue(record, cell);
                } else {
                    // 根据列名称获取值
                    // 对 rowkey 的读取，需要考虑 isBinaryRowkey；而对普通字段的读取，isBinaryRowkey 无影响

                    columnName = cell.getColumnName();

                    if (isRowkeyColumn(columnName)) {
                        doFillRecord(result.getRow(), columnType, this.isBinaryRowkey,
                                this.encoding, cell.getDateformat(), record);
                    } else {
                        cf = cell.getCf();
                        qualifier = cell.getQualifier();
                        tempValue = result.getValue(cf, qualifier);

                        doFillRecord(tempValue, columnType, false, this.encoding,
                                cell.getDateformat(), record);
                    }
                }
            }
        } catch (Exception e) {
            // 注意，这里catch的异常，期望是byte数组转换失败的情况。而实际上，string的byte数组，转成整数类型是不容易报错的。但是转成double类型容易报错。

            record.setColumn(0, new StringColumn(this.bytesToString(result.getRow(), this.isBinaryRowkey, this.encoding)));

            throw e;
        }

        return true;
    }

    private boolean isRowkeyColumn(String columnName) {
        return "rowkey".equalsIgnoreCase(columnName);
    }

    private void doFillRecord(byte[] byteArray, ColumnType columnType, boolean isBinaryRowkey, String encoding, String dateformat, Record record) throws Exception {
        switch (columnType) {
            case BOOLEAN:
                record.addColumn(new BoolColumn(Bytes.toBoolean(byteArray)));
                break;
            case SHORT:
                record.addColumn(new LongColumn(String.valueOf(Bytes.toShort(byteArray))));
                break;
            case INT:
                record.addColumn(new LongColumn(Bytes.toInt(byteArray)));
                break;
            case LONG:
                record.addColumn(new LongColumn(Bytes.toLong(byteArray)));
                break;
            case BYTES:
                record.addColumn(new BytesColumn(byteArray));
                break;
            case FLOAT:
                record.addColumn(new DoubleColumn(Bytes.toFloat(byteArray)));
                break;
            case DOUBLE:
                record.addColumn(new DoubleColumn(Bytes.toDouble(byteArray)));
                break;
            case STRING:
                record.addColumn(new StringColumn(bytesToString(byteArray, isBinaryRowkey, encoding)));
                break;

            case DATE:
                String dateValue = this.bytesToString(byteArray, isBinaryRowkey, encoding);
                record.addColumn(new DateColumn(DateUtils.parseDate(dateValue, new String[]{dateformat})));
                break;
            default:
                throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, "");
        }
    }


    private String bytesToString(byte[] byteArray, boolean isBinaryRowkey, String encoding) throws Exception {
        if (byteArray == null) {
            return null;
        }

        if (isBinaryRowkey) {
            return Bytes.toStringBinary(byteArray);
        } else {
            return new String(byteArray, encoding);
        }
    }

    private void fillRecordWithConstantValue(Record record, HbaseColumnCell cell) throws Exception {
        String constantValue = cell.getColumnValue();
        ColumnType columnType = cell.getColumnType();
        switch (columnType) {
            case BOOLEAN:
                record.addColumn(new BoolColumn(constantValue));
                break;
            case SHORT:
            case INT:
            case LONG:
                record.addColumn(new LongColumn(constantValue));
                break;
            case BYTES:
                record.addColumn(new BytesColumn(constantValue.getBytes("utf-8")));
                break;
            case FLOAT:
            case DOUBLE:
                record.addColumn(new DoubleColumn(constantValue));
                break;
            case STRING:
                record.addColumn(new StringColumn(constantValue));
                break;
            case DATE:
                record.addColumn(new DateColumn(DateUtils.parseDate(constantValue, new String[]{cell.getDateformat()})));
                break;
            default:
                throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, "");
        }
    }

    public void close() throws IOException {
        if (this.resultScanner != null) {
            this.resultScanner.close();
        }
        if (this.htable != null) {
            htable.close();
            HTableFactory.closeHtable();
        }
    }

    private void check() throws DataXException, IOException {
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

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

}
