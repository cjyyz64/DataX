package com.alibaba.datax.plugin.writer.otswriter.utils;

import java.security.InvalidParameterException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSPKColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.Pair;
import com.aliyun.openservices.ots.internal.model.ColumnValue;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.RowPutChange;
import com.aliyun.openservices.ots.internal.model.RowUpdateChange;

public class ParseRecord {
    
    private static final Logger LOG = LoggerFactory.getLogger(ParseRecord.class);
    
    private static com.aliyun.openservices.ots.internal.model.Column buildColumn(String name, ColumnValue value, long timestamp) {
        if (timestamp > 0) {
            return new com.aliyun.openservices.ots.internal.model.Column(
                    name, 
                    value,
                    timestamp
                    );
        } else {
            return new com.aliyun.openservices.ots.internal.model.Column(
                    name, 
                    value
                    );
        }
    }
    /**
     * 基于普通方式处理Record
     * @param tableName
     * @param type
     * @param pkColumns
     * @param attrColumns
     * @param record
     * @param timestamp
     * @param collector
     * @return
     */
    public static OTSLine parseNormalRecordToOTSLine(
            String tableName, 
            OTSOpType type, 
            List<OTSPKColumn> pkColumns, 
            List<OTSAttrColumn> attrColumns,
            Record record,
            long timestamp,
            TaskPluginCollector collector) {
        
        PrimaryKey pk = null;
        List<Pair<String, ColumnValue>> values = null;
        
        try {
            pk = Common.getPKFromRecord(pkColumns, record);
            values = Common.getAttrFromRecord(pkColumns.size(), attrColumns, record);
        } catch (IllegalArgumentException e) {
            collector.collectDirtyRecord(record, e.getMessage());
            return null;
        }
        
        switch (type) {
            case PUT_ROW:
                RowPutChange rowPutChange = new RowPutChange(tableName, pk);
                for (Pair<String, ColumnValue> en : values) {
                    if (en.getValue() != null) {
                        rowPutChange.addColumn(buildColumn(en.getKey(), en.getValue(), timestamp));
                    } 
                }
                return new OTSLine(pk, record, rowPutChange);
            case UPDATE_ROW:
                RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, pk);
                for (Pair<String, ColumnValue> en : values) {
                    if (en.getValue() != null) {
                        rowUpdateChange.put(buildColumn(en.getKey(), en.getValue(), timestamp));
                    } else {
                        rowUpdateChange.deleteColumns(en.getKey()); // 删除整列
                    }
                }
                return new OTSLine(pk, record, rowUpdateChange);
            default:
                throw new IllegalArgumentException(String.format(OTSErrorMessage.UNSUPPORT_PARSE, type, "RowChange"));
        }
    }
    
    public static OTSAttrColumn getDefineColumn(Map<String, OTSAttrColumn> attrColumnMapping, int columnNameIndex, Record r) {
        String columnName = r.getColumn(columnNameIndex).asString();
        OTSAttrColumn col = attrColumnMapping.get(columnName);
        if (col == null) {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.COLUMN_NOT_DEFINE, columnName));
        }
        return col;
    }
    
    private static com.aliyun.openservices.ots.internal.model.Column parseToColumn(
            List<OTSPKColumn> pkColumns,
            Map<String, OTSAttrColumn> attrColumnMapping,
            Record r) {
        
        OTSAttrColumn meta = getDefineColumn(attrColumnMapping, pkColumns.size(), r);
        Column timestamp = r.getColumn(pkColumns.size() + 1);
        Column value = r.getColumn(pkColumns.size() + 2);
        
        if (timestamp.getRawData() == null) {
            throw new IllegalArgumentException(OTSErrorMessage.MULTI_VERSION_TIMESTAMP_IS_EMPTY);
        }
        
        if (value.getRawData() == null) {
            return null;
        }
        
        ColumnValue otsValue = ColumnConversion.columnToColumnValue(value, meta);
        
        return new com.aliyun.openservices.ots.internal.model.Column(
                meta.getName(), 
                otsValue,
                timestamp.asLong()
                );
    }
    
    /**
     * 基于特殊模式处理Record
     * @param tableName
     * @param type
     * @param pkColumns
     * @param attrColumns
     * @param records
     * @param collector
     * @return
     * @throws Exception
     */
    public static OTSLine parseMultiVersionRecordToOTSLine(
            String tableName, 
            OTSOpType type, 
            List<OTSPKColumn> pkColumns, 
            List<OTSAttrColumn> attrColumns,
            List<Record> records, 
            TaskPluginCollector collector) throws Exception {
        
        Map<String, OTSAttrColumn> attrColumnMapping = new LinkedHashMap<String, OTSAttrColumn>();
        for (OTSAttrColumn c : attrColumns) {
            attrColumnMapping.put(c.getSrcName(), c);
        }
        
        if (records.isEmpty()) {
            throw new InvalidParameterException(OTSErrorMessage.INPUT_RECORDS_IS_EMPTY);
        }
        
        PrimaryKey pk = null;
        
        try {
            pk = Common.getPKFromRecord(pkColumns, records.get(0));
        } catch (RuntimeException e) {
            LOG.warn("RuntimeException: {}", e.getMessage(), e);
            // PK 转换失败，整体个Row失败
            Common.collectDirtyRecord(collector, e.getMessage(), records); 
            return null;
        }
        
        try {
            switch(type) {
                case PUT_ROW:
                    RowPutChange putChange = new RowPutChange(tableName, pk);
                    for (Record r : records) {
                        com.aliyun.openservices.ots.internal.model.Column c = null;
                        try {
                            c = parseToColumn(pkColumns, attrColumnMapping, r);
                        } catch (RuntimeException e) {
                            LOG.warn("RuntimeException: {}", e.getMessage(), e);
                            collector.collectDirtyRecord(r, e.getMessage());
                            continue;
                        }
                        putChange.addColumn(c);
                    }
                    return new OTSLine(type, pk, records, putChange);
                case UPDATE_ROW:
                    RowUpdateChange updateChange = new RowUpdateChange(tableName, pk);
                    for (Record r : records) {
                        
                        com.aliyun.openservices.ots.internal.model.Column c = null;
                        try {
                            c = parseToColumn(pkColumns, attrColumnMapping, r);
                        } catch (RuntimeException e) {
                            LOG.warn("RuntimeException: {}", e.getMessage(), e);
                            collector.collectDirtyRecord(r, e.getMessage());
                            continue;
                        }
                        
                        // TODO 这只是一个临时方法，后面需要考虑怎么处理当Value为空得情况
                        if (c == null) {
                            OTSAttrColumn meta = getDefineColumn(attrColumnMapping, pkColumns.size(), r);
                            Column timestamp = r.getColumn(pkColumns.size() + 1);
                            updateChange.deleteColumn(meta.getName(), timestamp.asLong());
                        } else {
                            updateChange.put(c);
                        }
                    }
                    return new OTSLine(type, pk, records, updateChange);
                default:
                    throw new Exception(String.format(OTSErrorMessage.UNSUPPORT_PARSE, type, "RowChange"));
            }
        } catch (Exception e) {
            LOG.warn("Exception: {}", e.getMessage(), e);
            Common.collectDirtyRecord(collector, e.getMessage(), records); 
            return null;
        }
    }
}
