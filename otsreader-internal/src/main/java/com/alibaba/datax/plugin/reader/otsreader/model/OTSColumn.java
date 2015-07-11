package com.alibaba.datax.plugin.reader.otsreader.model;

import com.aliyun.openservices.ots.internal.model.ColumnType;
import com.aliyun.openservices.ots.internal.model.ColumnValue;

public class OTSColumn {
    private String name;
    private ColumnValue value;
    private ColumnType valueType;
    
    private OTSColumnType columnType;

    public static enum OTSColumnType {
        NORMAL, // 普通列
        CONST   // 常量列
    }
    
    private OTSColumn(String name) {
        this.name = name;
        this.columnType = OTSColumnType.NORMAL;
    }
    
    private OTSColumn(ColumnValue value, ColumnType type) {
        this.value = value;
        this.columnType = OTSColumnType.CONST;
        this.valueType = type;
    }
    
    public static OTSColumn fromNormalColumn(String name) {
        if (name.isEmpty()) {
            throw new IllegalArgumentException("The column name is empty.");
        }
        
        return new OTSColumn(name);
    } 
    
    public static OTSColumn fromConstStringColumn(String value) {
        return new OTSColumn(ColumnValue.fromString(value), ColumnType.STRING);
    } 
    
    public static OTSColumn fromConstIntegerColumn(long value) {
        return new OTSColumn(ColumnValue.fromLong(value), ColumnType.INTEGER);
    } 
    
    public static OTSColumn fromConstDoubleColumn(double value) {
        return new OTSColumn(ColumnValue.fromDouble(value), ColumnType.DOUBLE);
    } 
    
    public static OTSColumn fromConstBoolColumn(boolean value) {
        return new OTSColumn(ColumnValue.fromBoolean(value), ColumnType.BOOLEAN);
    } 
    
    public static OTSColumn fromConstBytesColumn(byte[] value) {
        return new OTSColumn(ColumnValue.fromBinary(value), ColumnType.BINARY);
    } 
    
    public ColumnValue getValue() {
        return value;
    }
    
    public OTSColumnType getColumnType() {
        return columnType;
    }
    
    public ColumnType getValueType() {
        return valueType;
    }

    public String getName() {
        return name;
    }
}