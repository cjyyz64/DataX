package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.StringColumn;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;

public class TranformHelper {
    
    public static Column otsPrimaryKeyColumnToDataxColumn(PrimaryKeyColumn pkc) {
        switch (pkc.getValue().getType()) {
            case STRING:return new StringColumn(pkc.getValue().asString());
            case INTEGER:return new LongColumn(pkc.getValue().asLong()); 
            case BINARY:return new BytesColumn(pkc.getValue().asBinary()); 
            default:
                throw new IllegalArgumentException("PrimaryKey unsuporrt tranform the type: " + pkc.getValue().getType() + ".");
        }
    }
    
    public static Column otsColumnToDataxColumn(com.aliyun.openservices.ots.internal.model.Column c) {
        switch (c.getValue().getType()) {
            case STRING:return new StringColumn(c.getValue().asString());
            case INTEGER:return new LongColumn(c.getValue().asLong());
            case BINARY:return new BytesColumn(c.getValue().asBinary());
            case BOOLEAN:return new BoolColumn(c.getValue().asBoolean());
            case DOUBLE:return new DoubleColumn(c.getValue().asDouble());
            default:
                throw new IllegalArgumentException("Column unsuporrt tranform the type: " + c.getValue().getType() + ".");
            
        }
    }
}
