package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.ColumnType;
import com.alibaba.datax.plugin.reader.hbasereader.HbaseReaderErrorCode;
import com.alibaba.datax.plugin.reader.hbasereader.Key;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public abstract class MultiVersionTask extends HbaseAbstractTask {
    private static byte[] COLON_BYTE;
    private int maxVersion;
    private List<KeyValue> kvList = new ArrayList<KeyValue>();
    private int currentReadPosition = 0;

    // 四元组的类型
    private ColumnType rowkeyReadoutType = null;
    private ColumnType columnReadoutType = null;
    private ColumnType timestampReadoutType = null;
    private ColumnType valueReadoutType = null;

    public MultiVersionTask(Configuration configuration) throws Exception {
        super(configuration);

        this.maxVersion = configuration.getInt(Key.MAX_VERSION);
        List<String> userConfiguredTetradTypes = configuration.getList(Key.TETRAD_TYPE, String.class);

        this.rowkeyReadoutType = ColumnType.getByTypeName(userConfiguredTetradTypes.get(0));
        this.columnReadoutType = ColumnType.getByTypeName(userConfiguredTetradTypes.get(1));
        this.timestampReadoutType = ColumnType.getByTypeName(userConfiguredTetradTypes.get(2));
        this.valueReadoutType = ColumnType.getByTypeName(userConfiguredTetradTypes.get(3));

        MultiVersionTask.COLON_BYTE = ":".getBytes("utf8");
    }

    private void convertKVToLine(KeyValue keyValue, Record record) throws Exception {
        byte[] rawRowkey = keyValue.getRow();

        long timestamp = keyValue.getTimestamp();

        byte[] cfAndQualifierName = Bytes.add(keyValue.getFamily(), MultiVersionTask.COLON_BYTE, keyValue.getQualifier());

        record.addColumn(convertBytesToAssignType(this.rowkeyReadoutType, rawRowkey));

        record.addColumn(convertBytesToAssignType(this.columnReadoutType, cfAndQualifierName));

        // 直接忽略了用户配置的 timestamp 的类型
        record.addColumn(new LongColumn(timestamp));

        record.addColumn(convertBytesToAssignType(this.valueReadoutType, keyValue.getValue()));
    }

    private Column convertBytesToAssignType(ColumnType columnType, byte[] byteArray) throws UnsupportedEncodingException {
        Column column;
        switch (columnType) {
            case BOOLEAN:
                column = new BoolColumn(Bytes.toBoolean(byteArray));
                break;
            case SHORT:
                column = new LongColumn(String.valueOf(Bytes.toShort(byteArray)));
                break;
            case INT:
                column = new LongColumn(Bytes.toInt(byteArray));
                break;
            case LONG:
                column = new LongColumn(Bytes.toLong(byteArray));
                break;
            case BYTES:
                column = new BytesColumn(byteArray);
                break;
            case FLOAT:
                column = new DoubleColumn(Bytes.toFloat(byteArray));
                break;
            case DOUBLE:
                column = new DoubleColumn(Bytes.toDouble(byteArray));
                break;
            case STRING:
                column = new StringColumn(byteArray == null ? null : new String(byteArray, super.encoding));
                break;
            case BINARY_STRING:
                column = new StringColumn(Bytes.toStringBinary(byteArray));
                break;

            default:
                throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 不支持您配置的列类型:" + columnType);
        }

        return column;
    }

    @Override
    public boolean fetchLine(Record record) throws Exception {
        Result result;
        if (this.kvList.size() == this.currentReadPosition) {
            result = super.getNextHbaseRow();
            if (result == null) {
                return false;
            }

            this.kvList = result.list();
            if (this.kvList == null) {
                return false;
            }

            this.currentReadPosition = 0;
        }

        try {
            KeyValue keyValue = this.kvList.get(this.currentReadPosition);
            convertKVToLine(keyValue, record);
        } catch (Exception e) {
            throw e;
        } finally {
            this.currentReadPosition++;
        }

        return true;
    }

    public void setMaxVersions(Scan scan) {
        if (this.maxVersion == -1 || this.maxVersion == Integer.MAX_VALUE) {
            scan.setMaxVersions();
        } else {
            scan.setMaxVersions(this.maxVersion);
        }
    }

}
