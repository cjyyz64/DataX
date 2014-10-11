package com.alibaba.datax.plugin.reader.otsreader.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConst;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

/**
 * 主要对OTS PrimaryKey，OTSColumn的解析
 */
public class ReaderModelParser {
    
    public static OTSColumn parseConstColumn(String type, String value) {
        if (type.equalsIgnoreCase(OTSConst.TYPE_STRING)) {
            return OTSColumn.fromConstStringColumn(value);
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_INTEGER)) {
            return OTSColumn.fromConstIntegerColumn(Long.parseLong(value));
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_DOUBLE)) {
            return OTSColumn.fromConstDoubleColumn(Double.parseDouble(value));
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_BOOLEAN)) {
            return OTSColumn.fromConstBoolColumn(Boolean.parseBoolean(value));
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_BINARY)) {
            return OTSColumn.fromConstBytesColumn(Base64.decodeBase64(value));
        } else {
            throw new IllegalArgumentException("Can not parse map to 'ColumnValue', input type:" + type + ", value:" + value + ".");
        }
    }
    
    public static OTSColumn parseOTSColumn(Map<String, Object> item) {
        if (item.containsKey(OTSConst.NAME)) {
            Object name = item.get(OTSConst.NAME);
            if (name instanceof String) {
                String nameStr = (String) name;
                return OTSColumn.fromNormalColumn(nameStr);
            } else {
                throw new IllegalArgumentException("Can not parse map to 'OTSColumn', the value is not a string.");
            }
        } else if (item.containsKey(OTSConst.TYPE) && item.containsKey(OTSConst.VALUE)) {
            Object type = item.get(OTSConst.TYPE);
            Object value = item.get(OTSConst.VALUE);
            if (type instanceof String && value instanceof String) {
                String typeStr = (String) type;
                String valueStr = (String) value;
                return parseConstColumn(typeStr, valueStr);
            } else {
                throw new IllegalArgumentException("Can not parse map to 'OTSColumn', the value is not a string.");
            }
        } else {
            throw new IllegalArgumentException(
                    "Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'. ");
        }
    }

    public static List<OTSColumn> parseOTSColumnList(List<Object> input) {
        if (input.isEmpty()) {
            throw new IllegalArgumentException("Input count of column is zero.");
        }
        
        List<OTSColumn> columns = new ArrayList<OTSColumn>(input.size());
        
        for (Object item:input) {
            if (item instanceof Map){ 
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) item;
                columns.add(parseOTSColumn(column));
            } else {
                throw new IllegalArgumentException("Can not parse Object to 'OTSColumn', item of list is not a map.");
            }
        }
        
        return columns;
    }
    
    public static PrimaryKeyValue parsePrimaryKeyValue(String type, String value) {
        try {
            if (type.equalsIgnoreCase(OTSConst.TYPE_STRING)) {
                return PrimaryKeyValue.fromString(value);
            } else if (type.equalsIgnoreCase(OTSConst.TYPE_INTEGER)) {
                return PrimaryKeyValue.fromLong(Long.parseLong(value));
            } else if (type.equalsIgnoreCase(OTSConst.TYPE_INF_MIN)) {
                return PrimaryKeyValue.INF_MIN;
            } else if (type.equalsIgnoreCase(OTSConst.TYPE_INF_MAX)) {
                return PrimaryKeyValue.INF_MAX;
            } else {
                throw new IllegalArgumentException("Can not parse String to 'PrimaryKeyValue'. input type:" + type + ", value:" + value + ".");
            }
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Can not parse the value '"+ value +"' to "+ type +".");
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
                throw new IllegalArgumentException("The 'type' and 'value only support string.'");
            }
        } else {
            throw new IllegalArgumentException("The map must include 'type' and 'value'.");
        }
    }

    public static List<PrimaryKeyValue> parsePrimaryKey(List<Object> input) {
        if (null == input) {
            return null;
        }
        List<PrimaryKeyValue> columns = new ArrayList<PrimaryKeyValue>(input.size());
        for (Object item:input) {
            if (item instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) item;
                columns.add(parsePrimaryKeyValue(column));
            } else {
                throw new IllegalArgumentException("Can not parse Object to 'PrimaryKeyValue', item of list is not a map.");
            }
        }
        return columns;
    }
}
