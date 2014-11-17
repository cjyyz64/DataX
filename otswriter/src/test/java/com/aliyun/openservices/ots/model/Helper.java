package com.aliyun.openservices.ots.model;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.OTSException;

public class Helper {
    
    public static long getPKSize(Map<String, PrimaryKeyValue> pks) {
        int rowSize = 0;
        for(Entry<String, PrimaryKeyValue> pk : pks.entrySet()) {
            rowSize += pk.getKey().length();
            switch (pk.getValue().getType()) {
                case INTEGER:
                    rowSize += 8;
                    break;
                case STRING:
                    rowSize += pk.getValue().asString().length();
                    break;
                default:
                    break;
            }
        }
        return rowSize;
    }
    
    public static long getAttrSize(Map<String, ColumnValue> attrs) {
        int rowSize = 0;
        for (Entry<String, ColumnValue> attr : attrs.entrySet()) {
            //检查列名
            if (!Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*").matcher(attr.getKey()).matches()) {
                throw new OTSException(
                        "Column name invalid", 
                        null, 
                        OTSErrorCode.INVALID_PARAMETER, 
                        "RequestId",
                        400);
            }
            rowSize += attr.getKey().length();
            if (attr.getValue() != null) {
                switch (attr.getValue().getType()) {
                    case BINARY:
                        rowSize += attr.getValue().asBinary().length;
                        break;
                    case BOOLEAN:
                        rowSize += 1;
                        break;
                    case DOUBLE:
                        rowSize += 8;
                        break;
                    case INTEGER:
                        rowSize += 8;
                        break;
                    case STRING:
                        rowSize += attr.getValue().asString().length();
                        break;
                    default:
                        break;
                        }
            }
        }
        return rowSize;
    }
    
    public static long getRowSize(Map<String, PrimaryKeyValue> pks, Map<String, ColumnValue> attrs) {
        return getPKSize(pks) + getAttrSize(attrs);
    }

    public static long getCU(Map<String, PrimaryKeyValue> pks, Map<String, ColumnValue> attrs) {
        long rowSize = getRowSize(pks, attrs);
        long expectCU = rowSize % 1024 > 0 ? rowSize / 1024 + 1 : rowSize / 1024;
        return expectCU;
    }
    
    public static Row buildRow(OTSOpType type, Map<String, PrimaryKeyValue> pk, Map<String, ColumnValue> attr, Map<String, ColumnValue> oldAttr) {
        Row row = new Row();
        for (Entry<String, PrimaryKeyValue> en : pk.entrySet()) {
            switch (en.getValue().getType()) {
                case INTEGER:
                    row.addColumn(en.getKey(), ColumnValue.fromLong(en.getValue().asLong()));
                    break;
                case STRING:
                    row.addColumn(en.getKey(), ColumnValue.fromString(en.getValue().asString()));
                    break;
                default:
                    break;
            }
        }
        
        Map<String, ColumnValue> tmpAttr = new LinkedHashMap<String, ColumnValue>();
        
        if (type == OTSOpType.UPDATE_ROW && oldAttr != null) {
            for (Entry<String, ColumnValue> en : oldAttr.entrySet()) {
                tmpAttr.put(en.getKey(), en.getValue());
            }
        }
        
        for (Entry<String, ColumnValue> en : attr.entrySet()) {
            if (en.getValue() == null) {
                tmpAttr.remove(en.getKey());
            } else {
                tmpAttr.put(en.getKey(), en.getValue());
            }
        }
        
        for (Entry<String, ColumnValue> en : tmpAttr.entrySet()) {
            row.addColumn(en.getKey(), en.getValue());
        }
        return row;
    }
}
