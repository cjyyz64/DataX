package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.impl.cookie.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public abstract class HbaseAbstractReader {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseAbstractReader.class);

    private static final String META_SCANNER_CACHING = "100";

    private org.apache.hadoop.conf.Configuration config;

    private byte[] startKey = null;
    private int scanCache;
    private byte[] endKey = null;

    protected List<HbaseColumnCell> hbaseColumnCells;
    protected HTable htable;
    protected String encoding;
    protected boolean isBinaryRowkey;
    protected Result lastResult = null;
    protected Scan scan;
    protected ResultScanner resultScanner;

    public HbaseAbstractReader(Configuration configuration) {
        String userConfiguredHbaseSiteConfig = configuration.getString(Key.HBASE_CONFIG);
        String tableName = configuration.getString(Key.TABLE);
        this.encoding = configuration.getString(Key.ENCODING);
        this.isBinaryRowkey = configuration.getBool(Key.IS_BINARY_ROWKEY);

        this.scanCache = configuration.getInt(Key.SCAN_CACHE, Constant.DEFAULT_SCAN_CACHE);

        org.apache.hadoop.conf.Configuration conf = HbaseUtil.getHbaseConf(userConfiguredHbaseSiteConfig);
        this.config = new org.apache.hadoop.conf.Configuration(conf);

        this.config.set("hbase.meta.scanner.caching", META_SCANNER_CACHING);

        this.htable = HbaseUtil.initHtable(configuration);

        this.startKey = HbaseUtil.getStartRowKey(configuration);
        this.endKey = HbaseUtil.getEndRowKey(configuration);

    }

    public abstract boolean fetchLine(Record record) throws Exception;

    public void prepare(List<HbaseColumnCell> hbaseColumnCells) throws Exception {
        this.hbaseColumnCells = hbaseColumnCells;

        this.scan = new Scan();
        scan.setCacheBlocks(false);

        this.scan.setStartRow(startKey);
        this.scan.setStopRow(endKey);

        LOG.info("The task set startkey=[{}], endkey=[{}] .", bytesToString(this.startKey, this.isBinaryRowkey, this.encoding), bytesToString(this.endKey, this.isBinaryRowkey, this.encoding));

        boolean isConstant;
        boolean isRowkeyColumn;
        for (HbaseColumnCell cell : hbaseColumnCells) {
            isConstant = cell.isConstant();
            isRowkeyColumn = this.isRowkeyColumn(cell.getColumnName());

            if (!isConstant && !isRowkeyColumn) {
                this.scan.addColumn(cell.getCf(), cell.getQualifier());
            }
        }

        initScan(this.scan);

        this.scan.setCaching(this.scanCache);
        this.resultScanner = this.htable.getScanner(this.scan);
    }

    public abstract void initScan(Scan scan);

    public void close() throws IOException {
        if (this.resultScanner != null) {
            this.resultScanner.close();
        }
        if (this.htable != null) {
            htable.close();
            HTableFactory.closeHtable();
        }
    }


    protected String bytesToString(byte[] byteArray, boolean isBinaryRowkey, String encoding) throws Exception {
        if (byteArray == null) {
            return null;
        }

        if (isBinaryRowkey) {
            return Bytes.toStringBinary(byteArray);
        } else {
            return new String(byteArray, encoding);
        }
    }

    protected boolean isRowkeyColumn(String columnName) {
        return "rowkey".equalsIgnoreCase(columnName);
    }

    protected void doFillRecord(byte[] byteArray, ColumnType columnType, boolean isBinaryRowkey, String encoding, String dateformat, Record record) throws Exception {
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
                String dateValue = bytesToString(byteArray, isBinaryRowkey, encoding);
                record.addColumn(new DateColumn(DateUtils.parseDate(dateValue, new String[]{dateformat})));
                break;
            default:
                throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, "");
        }
    }
}
