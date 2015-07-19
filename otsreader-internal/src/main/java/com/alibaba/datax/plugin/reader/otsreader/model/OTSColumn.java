package com.alibaba.datax.plugin.reader.otsreader.model;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.StringColumn;
import com.aliyun.openservices.ots.internal.model.ColumnType;

public class OTSColumn {
    private String name;
    private Column value;
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
    
    private OTSColumn(Column value) {
        this.value = value;
        this.columnType = OTSColumnType.CONST;
    }
    
    public static OTSColumn fromNormalColumn(String name) {
        if (name.isEmpty()) {
            throw new IllegalArgumentException("The column name is empty.");
        }
        
        return new OTSColumn(name);
    } 
    
    public static OTSColumn fromConstStringColumn(String value) {
        return new OTSColumn(new StringColumn(value));
    } 
    
    public static OTSColumn fromConstIntegerColumn(long value) {
        return new OTSColumn(new LongColumn(value));
    } 
    
    public static OTSColumn fromConstDoubleColumn(double value) {
        return new OTSColumn(new DoubleColumn(value));
    } 
    
    public static OTSColumn fromConstBoolColumn(boolean value) {
        return new OTSColumn(new BoolColumn(value));
    } 
    
    public static OTSColumn fromConstBytesColumn(byte[] value) {
        return new OTSColumn(new BytesColumn(value));
    } 
    
    public Column getValue() {
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