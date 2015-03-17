package com.alibaba.datax.plugin.writer.otswriter.common;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.openservices.ots.internal.model.Column;
import com.aliyun.openservices.ots.internal.model.ColumnValue;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.internal.model.Row;

public class OTSRowBuilder {
    
    private List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
    private List<Column> attrs = new ArrayList<Column>();
    
    private OTSRowBuilder() {}
    
    public static OTSRowBuilder newInstance() {
        return new OTSRowBuilder();
    }
    
    public OTSRowBuilder addPrimaryKeyColumn(String name, PrimaryKeyValue value) {
        primaryKeyColumn.add(new PrimaryKeyColumn(name, value));
        return this;
    }
    
//    public OTSRowBuilder addAttrColumn(String name, ColumnValue value) {
//        attrs.add(new Column(name, value));
//        return this;
//    }
    
    public OTSRowBuilder addAttrColumn(String name, ColumnValue value, long ts) {
        attrs.add(new Column(name, value, ts));
        return this;
    }
    
    public Row toRow() {
        return new Row(new PrimaryKey(primaryKeyColumn), attrs);
    }
}
