package com.alibaba.datax.plugin.reader.otsreader.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.alibaba.datax.plugin.reader.otsreader.Constant;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSCriticalException;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;

public class ParamParser {
    
    // ------------------------------------------------------------------------
    // Range解析相关的逻辑
    // ------------------------------------------------------------------------
    
    private static PrimaryKeyValue  parsePrimaryKeyValue(String type) {
        return parsePrimaryKeyValue(type, null);
    }
    
    private static PrimaryKeyValue  parsePrimaryKeyValue(String type, String value) {
        if (type.equalsIgnoreCase(Constant.VALUE.PrimaryKeyColumnType.INF_MIN)) {
            return PrimaryKeyValue.INF_MIN;
        } else if (type.equalsIgnoreCase(Constant.VALUE.PrimaryKeyColumnType.INF_MAX)) {
            return PrimaryKeyValue.INF_MAX;
        } else {
            if (value != null) {
                if (type.equalsIgnoreCase(Constant.VALUE.PrimaryKeyColumnType.STRING)) {
                    return PrimaryKeyValue.fromString(value);
                } else if (type.equalsIgnoreCase(Constant.VALUE.PrimaryKeyColumnType.INTEGER)) {
                    return PrimaryKeyValue.fromLong(Long.valueOf(value));
                } else if (type.equalsIgnoreCase(Constant.VALUE.PrimaryKeyColumnType.BINARY)) {
                    return PrimaryKeyValue.fromBinary(Base64.decodeBase64(value));
                } else {
                    throw new IllegalArgumentException("the column type only support :['INF_MIN', 'INF_MAX', 'string', 'int', 'binary']");
                }
            } else {
                throw new IllegalArgumentException("the column is missing the field 'value', input 'type':" + type);
            }
        }
    }
    
    private static PrimaryKeyColumn parsePrimaryKeyColumn(Map<String, Object> item) {
        Object typeObj = item.get(Constant.KEY.PrimaryKeyColumn.TYPE);
        Object valueObj = item.get(Constant.KEY.PrimaryKeyColumn.VALUE);
        
        if (typeObj != null && valueObj != null) {
            if (typeObj instanceof String && valueObj instanceof String) {
                return new PrimaryKeyColumn(
                        Constant.VALUE.DEFAULT_NAME, 
                        parsePrimaryKeyValue((String)typeObj, (String)valueObj)
                        );
            } else {
                throw new IllegalArgumentException(
                        "the column's 'type' and 'value' must be string value, "
                                + "but type of 'type' is :" + typeObj.getClass() + 
                                ", type of 'value' is :" + valueObj.getClass()
                        );
            }
        } else if (typeObj != null) {
            if (typeObj instanceof String) {
                return new PrimaryKeyColumn(
                        Constant.VALUE.DEFAULT_NAME, 
                        parsePrimaryKeyValue((String)typeObj)
                        );
            } else {
                throw new IllegalArgumentException(
                        "the column's 'type' must be string value, "
                                + "but type of 'type' is :" + typeObj.getClass()
                        );
            }
        } else {
            throw new IllegalArgumentException("the column must include 'type' and 'value'.");
        }
    }
    
    @SuppressWarnings("unchecked")
    public static List<PrimaryKeyColumn> parsePrimaryKeyColumnArray(Object arrayObj) throws OTSCriticalException {
        try {
            List<PrimaryKeyColumn> columns = new  ArrayList<PrimaryKeyColumn>();
            if (arrayObj instanceof List) {
                List<Object> array = (List<Object>) arrayObj;
                for (Object o : array) {
                    if (o instanceof Map) {
                        Map<String, Object> column = (Map<String, Object>) o;
                        columns.add(parsePrimaryKeyColumn(column));
                    } else {
                        throw new IllegalArgumentException("input primary key column must be map object, but input type:" + o.getClass());
                    }
                }
            } else {
                throw new IllegalArgumentException("input 'begin','end','split' must be list object, but input type:" + arrayObj.getClass());
            }
            return columns;
        } catch (RuntimeException e) {
            // 因为基础模块本身可能抛出一些错误，为了方便定位具体的出错位置，在此把Range加入到Error Message中
            throw new OTSCriticalException("Parse 'range' fail, " + e.getMessage(), e);
        }
    }
    
    // ------------------------------------------------------------------------
    // Column解析相关的逻辑
    // ------------------------------------------------------------------------
    
    private static OTSColumn parseOTSColumn(Object obj) {
        if (obj instanceof String) {
            return OTSColumn.fromNormalColumn((String)obj);
        } else {
            throw new IllegalArgumentException("the 'name' must be string, but input:" + obj.getClass());
        }
    }
    
    private static OTSColumn parseOTSColumn(Object typeObj, Object valueObj) {
        if (typeObj instanceof String && valueObj instanceof String) {
            String type = (String)typeObj;
            String value = (String)valueObj;
            
            if (type.equalsIgnoreCase(Constant.VALUE.ColumnType.STRING)) {
                return OTSColumn.fromConstStringColumn(value);
            } else if (type.equalsIgnoreCase(Constant.VALUE.ColumnType.INTEGER)) {
                return OTSColumn.fromConstIntegerColumn(Long.valueOf(value));
            } else if (type.equalsIgnoreCase(Constant.VALUE.ColumnType.DOUBLE)) {
                return OTSColumn.fromConstDoubleColumn(Double.valueOf(value));
            } else if (type.equalsIgnoreCase(Constant.VALUE.ColumnType.BOOLEAN)) {
                return OTSColumn.fromConstBoolColumn(Boolean.valueOf(value));
            } else if (type.equalsIgnoreCase(Constant.VALUE.ColumnType.BINARY)) {
                return OTSColumn.fromConstBytesColumn(Base64.decodeBase64(value));
            } else {
                throw new IllegalArgumentException("the const column type only support :['string', 'int', 'double', 'boolean', 'binary']");
            }
        } else {
            throw new IllegalArgumentException("the 'type' and 'value' must be string, but 'type''s type:" + typeObj.getClass() + " 'value''s type:" + valueObj.getClass());
        }
    }
    
    private static OTSColumn parseOTSColumn(Map<String, Object> column) {
        Object typeObj = column.get(Constant.KEY.Column.TYPE);
        Object valueObj = column.get(Constant.KEY.Column.VALUE);
        Object nameObj = column.get(Constant.KEY.Column.NAME);
        
        if (nameObj != null) {
            return parseOTSColumn(nameObj);
        } else if (typeObj != null && valueObj != null) {
            return parseOTSColumn(typeObj, valueObj);
        } else {
            throw new IllegalArgumentException("the item of column format support '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'.");
        }
    }
    
    @SuppressWarnings("unchecked")
    public static List<OTSColumn> parseOTSColumnArray(List<Object> value) throws OTSCriticalException {
        try {
            List<OTSColumn> result = new ArrayList<OTSColumn>();
            for (Object item:value) {
                if (item instanceof Map){ 
                    Map<String, Object> column = (Map<String, Object>) item;
                    result.add(ParamParser.parseOTSColumn(column));
                } else {
                    throw new IllegalArgumentException("the item of column must be map object, but input: " + item.getClass());
                }
            }
            return result;
        } catch (RuntimeException e) {
            // 因为基础模块本身可能抛出一些错误，为了方便定位具体的出错位置，在此把Column加入到Error Message中
            throw new OTSCriticalException("Parse 'column' fail. " + e.getMessage(), e);
        }
    }
    
    // ------------------------------------------------------------------------
    // TimeRange解析相关的逻辑
    // ------------------------------------------------------------------------

    public static long parseTimeRangeItem(Object obj, String key) {
        if (obj instanceof Integer) {
            return (Integer)obj;
        } else if (obj instanceof Long) {
            return (Long)obj;
        } else {
            throw new IllegalArgumentException("the '"+ key +"' must be int, but input:" + obj.getClass());
        }
    }
}
