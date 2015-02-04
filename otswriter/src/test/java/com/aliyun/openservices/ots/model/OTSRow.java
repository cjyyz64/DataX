package com.aliyun.openservices.ots.model;

import java.util.LinkedHashMap;
import java.util.Map;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSRowPrimaryKey;

public class OTSRow {
    private Row row = new Row();
    private Map<String, PrimaryKeyValue> pk = new LinkedHashMap<String, PrimaryKeyValue>();
    
    public OTSRow() {}
    
    public void addPK(String name, PrimaryKeyValue value) {
        pk.put(name, value);
        switch (value.getType()) {
            case INTEGER:
                row.addColumn(name, ColumnValue.fromLong(value.asLong()));
                break;
            case STRING:
                row.addColumn(name, ColumnValue.fromString(value.asString()));
                break;
            default:
                break;}
        
    }
    
    public void addColumn(String name, ColumnValue value) {
        row.addColumn(name, value);
    }
    
    public OTSRowPrimaryKey getPK(){
        return new OTSRowPrimaryKey(pk);
    }
    
    public Row getRow() {
        return row;
    }
}
