package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderException;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.ColumnValueTransformHelper;
import com.aliyun.openservices.ots.internal.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleVerAndUpOnlyModeRecordSender implements IStreamRecordSender {

    private final RecordSender dataxRecordSender;
    private String shardId;
    private final boolean isExportSequenceInfo;
    private List<String> columnNames;

    public SingleVerAndUpOnlyModeRecordSender(RecordSender dataxRecordSender, String shardId, boolean isExportSequenceInfo, List<String> columnNames) {
        this.dataxRecordSender = dataxRecordSender;
        this.shardId = shardId;
        this.isExportSequenceInfo = isExportSequenceInfo;
        this.columnNames = columnNames;
    }

    @Override
    public void sendToDatax(StreamRecord streamRecord) {
        String sequenceInfo = getSequenceInfo(streamRecord);
        switch (streamRecord.getRecordType()) {
            case PUT:
            case UPDATE:
                sendToDatax(streamRecord.getPrimaryKey(), streamRecord.getColumns(), sequenceInfo);
                break;
            case DELETE:
                break;
            default:
                throw new OTSStreamReaderException("Unknown stream record type: " + streamRecord.getRecordType() + ".");
        }
    }

    private void sendToDatax(PrimaryKey primaryKey, List<RecordColumn> columns, String sequenceInfo) {
        Map<String, ColumnValue> map = new HashMap<String, ColumnValue>();
        for (RecordColumn recordColumn : columns) {
            if (recordColumn.getColumnType().equals(RecordColumn.ColumnType.PUT)) {
                map.put(recordColumn.getColumn().getName(), recordColumn.getColumn().getValue());
            }
        }

        if (map.isEmpty()) {
            return;
        }

        Record line = dataxRecordSender.createRecord();

        for (PrimaryKeyColumn pkCol : primaryKey.getPrimaryKeyColumns()) {
            line.addColumn(ColumnValueTransformHelper.otsPrimaryKeyValueToDataxColumn(pkCol.getValue()));
        }

        boolean findColumn  = false;
        for (String colName : columnNames) {
            ColumnValue value = map.get(colName);
            if (value != null) {
                findColumn = true;
                line.addColumn(ColumnValueTransformHelper.otsColumnValueToDataxColumn(value));
            } else {
                line.addColumn(null);
            }
        }

        if (!findColumn) {
            return;
        }

        if (isExportSequenceInfo) {
            line.addColumn(new StringColumn(sequenceInfo));
        }
        synchronized (dataxRecordSender) {
            dataxRecordSender.sendToWriter(line);
        }
    }

    private String getSequenceInfo(StreamRecord streamRecord) {
        int epoch = streamRecord.getSequenceInfo().getEpoch();
        long timestamp = streamRecord.getSequenceInfo().getTimestamp();
        int rowIdx = streamRecord.getSequenceInfo().getRowIndex();
        String sequenceId = String.format("%010d_%020d_%010d_%s", epoch, timestamp, rowIdx, shardId);
        return sequenceId;
    }
}
