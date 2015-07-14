package com.alibaba.datax.plugin.reader.otsreader.common;

import java.util.List;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.aliyun.openservices.ots.internal.model.Column;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.Row;

public class ExchangeHelper {
    public static Record parseRowToRecord(List<OTSColumn> columns, Row row) {
        Record line = new DefaultRecord();
        for (OTSColumn col : columns) {
            if (col.getColumnType() == OTSColumn.OTSColumnType.CONST) {
                line.addColumn(col.getValue());
            } else {
                Column v = row.getLatestColumn(col.getName());
                if (v == null) {
                    line.addColumn(new StringColumn(null));
                } else {
                    switch(v.getValue().getType()) {
                    case STRING:  line.addColumn(new StringColumn(v.getValue().asString())); break;
                    case INTEGER: line.addColumn(new LongColumn(v.getValue().asLong()));   break;
                    case DOUBLE:  line.addColumn(new DoubleColumn(v.getValue().asDouble())); break;
                    case BOOLEAN: line.addColumn(new BoolColumn(v.getValue().asBoolean()));  break;
                    case BINARY:  line.addColumn(new BytesColumn(v.getValue().asBinary()));  break;
                    default:
                        throw new IllegalArgumentException("Unsuporrt tranform the type: " + v.getValue().getType() + ".");
                    }
                }
            }
        }
        return line;
    }
    
    public static Record parseColumnToRecord(PrimaryKey pk, Column v) {
        Record line = new DefaultRecord();
        for (PrimaryKeyColumn pkc : pk.getPrimaryKeyColumns()) {
            switch (pkc.getValue().getType()) {
                case BINARY:line.addColumn(new BytesColumn(pkc.getValue().asBinary()));  break;
                case INTEGER:line.addColumn(new LongColumn(pkc.getValue().asLong()));   break;
                case STRING:line.addColumn(new StringColumn(pkc.getValue().asString())); break;
                default:
                    throw new IllegalArgumentException("Unsuporrt tranform the type: " + pkc.getValue().getType() + ".");
            }
        }
        line.addColumn(new StringColumn(v.getName()));
        line.addColumn(new LongColumn(v.getTimestamp()));
        switch(v.getValue().getType()) {
            case STRING:  line.addColumn(new StringColumn(v.getValue().asString())); break;
            case INTEGER: line.addColumn(new LongColumn(v.getValue().asLong()));   break;
            case DOUBLE:  line.addColumn(new DoubleColumn(v.getValue().asDouble())); break;
            case BOOLEAN: line.addColumn(new BoolColumn(v.getValue().asBoolean()));  break;
            case BINARY:  line.addColumn(new BytesColumn(v.getValue().asBinary()));  break;
            default:
                throw new IllegalArgumentException("Unsuporrt tranform the type: " + v.getValue().getType() + ".");
            }
        
        return line;
    }
}
