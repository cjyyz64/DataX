package com.alibaba.datax.plugin.reader.otsreader.model;

import com.aliyun.openservices.ots.model.ColumnValue;

public class OTSColumn {
    private ColumnValue value;
    private OTSColumnType type;
    
    public static enum OTSColumnType {
        NORMAL, // 普通列
        CONST   // 常量列
    }
    
    public OTSColumn(ColumnValue value, OTSColumnType type) {
        this.value = value;
        this.type = type;
    }
    
    public ColumnValue getValue() {
        return value;
    }
    
    public OTSColumnType getType() {
        return type;
    }
}
