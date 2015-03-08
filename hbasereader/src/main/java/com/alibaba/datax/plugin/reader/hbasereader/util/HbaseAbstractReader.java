package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.Constant;
import com.alibaba.datax.plugin.reader.hbasereader.HTableFactory;
import com.alibaba.datax.plugin.reader.hbasereader.HbaseColumnCell;
import com.alibaba.datax.plugin.reader.hbasereader.Key;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public abstract class HbaseAbstractReader {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseAbstractReader.class);

    private byte[] startKey = null;
    private int scanCache;
    private byte[] endKey = null;

    protected List<HbaseColumnCell> hbaseColumnCells;
    protected HTable htable;
    protected String encoding;
    protected Result lastResult = null;
    protected Scan scan;
    protected ResultScanner resultScanner;

    public HbaseAbstractReader(Configuration configuration) {
        this.htable = HbaseUtil.initHtable(configuration);

        this.encoding = configuration.getString(Key.ENCODING);

        this.scanCache = configuration.getInt(Key.SCAN_CACHE, Constant.DEFAULT_SCAN_CACHE);

        this.startKey = HbaseUtil.getStartRowKey(configuration);
        this.endKey = HbaseUtil.getEndRowKey(configuration);

    }

    public abstract boolean fetchLine(Record record) throws Exception;

    public abstract void setMaxVersions(Scan scan);

    public void prepare(List<HbaseColumnCell> hbaseColumnCells) throws Exception {
        this.hbaseColumnCells = hbaseColumnCells;

        this.scan = new Scan();
        scan.setCacheBlocks(false);

        this.scan.setStartRow(startKey);
        this.scan.setStopRow(endKey);

        LOG.info("The task set startRowkey=[{}], endRowkey=[{}].", Bytes.toStringBinary(this.startKey), Bytes.toStringBinary(this.endKey));

        boolean isConstant;
        boolean isRowkeyColumn;
        for (HbaseColumnCell cell : hbaseColumnCells) {
            isConstant = cell.isConstant();
            isRowkeyColumn = HbaseUtil.isRowkeyColumn(cell.getColumnName());

            if (!isConstant && !isRowkeyColumn) {
                this.scan.addColumn(cell.getCf(), cell.getQualifier());
            }
        }

        setMaxVersions(this.scan);

        this.scan.setCaching(this.scanCache);
        this.resultScanner = this.htable.getScanner(this.scan);
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

    protected Result getNextHbaseRow() throws IOException {
        Result result;
        try {
            result = resultScanner.next();
        } catch (IOException e) {
            if (lastResult != null) {
                scan.setStartRow(lastResult.getRow());
            }
            resultScanner = htable.getScanner(scan);
            result = resultScanner.next();
            if (lastResult != null && Bytes.equals(lastResult.getRow(), result.getRow())) {
                result = resultScanner.next();
            }
        }

        lastResult = result;

        // nay be null
        return result;
    }
}
