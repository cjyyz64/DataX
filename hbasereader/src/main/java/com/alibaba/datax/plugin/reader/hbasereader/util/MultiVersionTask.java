package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.Key;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public abstract class MultiVersionTask extends HbaseAbstractTask {
    private int maxVersion;
    private List<KeyValue> kvList = new ArrayList<KeyValue>();
    private int currentReadPosition = 0;

    public MultiVersionTask(Configuration configuration) {
        super(configuration);

        this.maxVersion = configuration.getInt(Key.MAX_VERSION);
    }

    private void convertKVToLine(KeyValue keyValue, Record record) throws Exception {
        byte[] rawRowkey = keyValue.getRow();
        long timestamp = keyValue.getTimestamp();

        String column = Bytes.toString(keyValue.getBuffer(), keyValue.getFamilyOffset(), keyValue.getFamilyLength()) + ":"
                + Bytes.toString(keyValue.getBuffer(), keyValue.getQualifierOffset(), keyValue.getQualifierLength());

        record.addColumn(new StringColumn(Bytes.toStringBinary(rawRowkey)));

        record.addColumn(new StringColumn(column));
        record.addColumn(new LongColumn(timestamp));

        record.addColumn(new StringColumn(Bytes.toStringBinary(keyValue.getValue())));
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
