package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.ColumnType;
import com.alibaba.datax.plugin.reader.hbasereader.HbaseColumnCell;
import com.alibaba.datax.plugin.reader.hbasereader.HbaseReaderErrorCode;
import com.alibaba.datax.plugin.reader.hbasereader.Key;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MutiVersionReader extends HbaseAbstractReader {
    private int maxVersion;
    private ColumnType rowkeyType;
    private List<KeyValue> kvList = new ArrayList<KeyValue>();
    private int currentReadPosition = 0;

    // key: columnName, value: HbaseColumnCell
    private Map<String, HbaseColumnCell> userConfigedColumnNameAndCellMap;

    public MutiVersionReader(Configuration configuration) {
        super(configuration);

        this.maxVersion = configuration.getInt(Key.MAX_VERSION);
        checkMaxVersion(this.maxVersion);

        String rowkeyTypeString = configuration.getString(Key.ROWKEY_TYPE);
        this.rowkeyType = ColumnType.getByTypeName(rowkeyTypeString);
    }

    private void checkMaxVersion(int maxVersion) {
        if (maxVersion != -1 && maxVersion < 2) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE,
                    "您配置的为 多版本读取模式（mutiVersion），但是 maxVersion 值不合法。maxVersion 的正确配置方式是： 配合 mode = mutiVersion 时使用，指明需要读取的版本个数。如果为-1: 表示去读全部版本; 不能为0或1; >1 表示最多读取对应个数的版本数(不能超过 Integer 的最大值)");
        }
    }


    private void convertKVToLine(KeyValue keyValue, Record record) throws Exception {
        byte[] rawRowkey = keyValue.getRow();
        long timestamp = keyValue.getTimestamp();

        String column = Bytes.toString(keyValue.getBuffer(), keyValue.getFamilyOffset(), keyValue.getFamilyLength()) + ":"
                + Bytes.toString(keyValue.getBuffer(), keyValue.getQualifierOffset(), keyValue.getQualifierLength());
        HbaseColumnCell columnCell = this.userConfigedColumnNameAndCellMap.get(column);

        super.doFillRecord(rawRowkey, this.rowkeyType, super.isBinaryRowkey,
                super.encoding, columnCell.getDateformat(), record);

        record.addColumn(new StringColumn(column));
        record.addColumn(new LongColumn(timestamp));

        super.doFillRecord(keyValue.getValue(), columnCell.getColumnType(), super.isBinaryRowkey,
                super.encoding, columnCell.getDateformat(), record);
    }

    @Override
    public void prepare(List<HbaseColumnCell> hbaseColumnCells) throws Exception {
        super.prepare(hbaseColumnCells);

        this.userConfigedColumnNameAndCellMap = new HashMap<String, HbaseColumnCell>();
        for (HbaseColumnCell hbaseColumnCell : hbaseColumnCells) {
            this.userConfigedColumnNameAndCellMap.put(hbaseColumnCell.getColumnName(), hbaseColumnCell);
        }
    }

    @Override
    public boolean fetchLine(Record record) throws Exception {
        Result result = null;
        if (this.kvList.size() == this.currentReadPosition) {
            result = getNextHbaseRow();
            if (result == null) {
                return false;
            }

            this.kvList = result.list();
            if (this.kvList == null) {
                return false;
            }

            this.currentReadPosition = 0;
        }

        KeyValue keyValue = this.kvList.get(this.currentReadPosition);
        convertKVToLine(keyValue, record);
        this.currentReadPosition++;

        return true;
    }

    @Override
    public void initScan(Scan scan) {
        if (this.maxVersion == -1 || this.maxVersion == Integer.MAX_VALUE) {
            scan.setMaxVersions();
        } else {
            scan.setMaxVersions(this.maxVersion);

        }
    }

    private Result getNextHbaseRow() throws IOException {
        Result result;
        try {
            result = super.resultScanner.next();
        } catch (IOException e) {
            if (super.lastResult != null) {
                super.scan.setStartRow(super.lastResult.getRow());
            }
            super.resultScanner = super.htable.getScanner(scan);
            result = super.resultScanner.next();
            if (super.lastResult != null && Bytes.equals(super.lastResult.getRow(), result.getRow())) {
                result = super.resultScanner.next();
            }
        }

        super.lastResult = result;

        // nay be null
        return result;
    }
}
