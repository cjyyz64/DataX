package com.alibaba.datax.plugin.reader.otsreader.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConst;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

/**
 * 主要对OTS PrimaryKey，ColumnValue的解析
 */
public class OtsModelUtils {
    public static ColumnValue parseColumnValue(String type, String value) {
        if (type.toUpperCase().equals(OTSConst.COLUMN_STRING)) {
            return ColumnValue.fromString(value);
        } else if (type.toUpperCase().equals(OTSConst.COLUMN_INTEGER)) {
            return ColumnValue.fromLong(Long.parseLong(value));
        } else if (type.toUpperCase().equals(OTSConst.COLUMN_DOUBLE)) {
            return ColumnValue.fromDouble(Double.parseDouble(value));
        } else if (type.toUpperCase().equals(OTSConst.COLUMN_BOOLEAN)) {
            return ColumnValue.fromBoolean(Boolean.parseBoolean(value));
        } else if (type.toUpperCase().equals(OTSConst.COLUMN_BINARY)) {
            return ColumnValue.fromBinary(Base64.decodeBase64(value));
        } else {
            throw new IllegalArgumentException(String.format("Unsupport parse."));
        }
    }
    
    public static OTSColumn parseOTSColumn(Map<String, Object> item) {
        if (item.containsKey(OTSConst.TYPE) && item.containsKey(OTSConst.VALUE)) {
            Object type = item.get(OTSConst.TYPE);
            Object value = item.get(OTSConst.VALUE);
            if (type instanceof String && value instanceof String) {
                String typeStr = (String) type;
                String valueStr = (String) value;
                return new OTSColumn(parseColumnValue(typeStr, valueStr), OTSColumn.OTSColumnType.CONST);
            } else {
                throw new IllegalArgumentException(String.format("Unsupport parse."));
            }
        } else {
            throw new IllegalArgumentException(String.format("Unsupport parse."));
        }
    }

    public static List<OTSColumn> parseOTSColumnList(List<Object> input) {
        if (input.isEmpty()) {
            throw new IllegalArgumentException(String.format("Input count of column is zero."));
        }
        
        List<OTSColumn> columns = new ArrayList<OTSColumn>(input.size());
        
        for (Object item:input) {
            if (item instanceof String) { // 普通列名
                String columnName = (String) item;
                columns.add(new OTSColumn(ColumnValue.fromString(columnName), OTSColumn.OTSColumnType.NORMAL));
            } else if (item instanceof Map){ // 常量列
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) item;
                columns.add(parseOTSColumn(column));
            } else {
                throw new IllegalArgumentException(String.format("Unsupport parse."));
            }
        }
        return columns;
    }
    
    public static PrimaryKeyValue parsePrimaryKeyValue(String type, String value) {
        if (type.toUpperCase().equals(OTSConst.PRIMARY_KEY_STRING)) {
            return PrimaryKeyValue.fromString(value);
        } else if (type.toUpperCase().equals(OTSConst.PRIMARY_KEY_INTEGER)) {
            return PrimaryKeyValue.fromLong(Long.parseLong(value));
        } else if (type.toUpperCase().equals(OTSConst.PRIMARY_KEY_INF_MIN)) {
            return PrimaryKeyValue.INF_MIN;
        } else if (type.toUpperCase().equals(OTSConst.PRIMARY_KEY_INF_MAX)) {
            return PrimaryKeyValue.INF_MAX;
        } else {
            throw new IllegalArgumentException(String.format("Unsupport parse."));
        }
    }
    
    public static PrimaryKeyValue parsePrimaryKeyValue(Map<String, Object> item) {
        if (item.containsKey(OTSConst.TYPE) && item.containsKey(OTSConst.VALUE)) {
            Object type = item.get(OTSConst.TYPE);
            Object value = item.get(OTSConst.VALUE);
            if (type instanceof String && value instanceof String) {
                String typeStr = (String) type;
                String valueStr = (String) value;
                return parsePrimaryKeyValue(typeStr, valueStr);
            } else {
                throw new IllegalArgumentException(String.format("Unsupport parse."));
            }
        } else {
            throw new IllegalArgumentException(String.format("Unsupport parse."));
        }
    }

    public static List<PrimaryKeyValue> parsePrimaryKey(List<Object> input) {
        List<PrimaryKeyValue> columns = new ArrayList<PrimaryKeyValue>(input.size());
        for (Object item:input) {
            if (item instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) item;
                columns.add(parsePrimaryKeyValue(column));
            } else {
                throw new IllegalArgumentException(String.format("Unsupport parse."));
            }
        }
        return columns;
    }
}
