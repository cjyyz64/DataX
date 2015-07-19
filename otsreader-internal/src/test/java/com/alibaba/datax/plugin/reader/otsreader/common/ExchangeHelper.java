package com.alibaba.datax.plugin.reader.otsreader.common;

import java.util.List;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.utils.TranformHelper;
import com.aliyun.openservices.ots.internal.model.Column;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.Row;

public class ExchangeHelper {
    public static Record parseRowToRecord(List<OTSColumn> columns, Row row) {
        Record line = new DefaultRecord();
        PrimaryKey pk = row.getPrimaryKey();
        for (OTSColumn col : columns) {
            if (col.getColumnType() == OTSColumn.OTSColumnType.CONST) {
                line.addColumn(col.getValue());
            } else {
                PrimaryKeyColumn pkc = pk.getPrimaryKeyColumn(col.getName());
                if (pkc != null) {
                    line.addColumn(TranformHelper.otsPrimaryKeyColumnToDataxColumn(pkc));
                } else {
                    Column c = row.getLatestColumn(col.getName());
                    if (c != null) {
                        line.addColumn(TranformHelper.otsColumnToDataxColumn(c));
                    } else {
                        line.addColumn(new StringColumn());
                    }
                }
            }
        }
        return line;
    }
    
    public static Record parseColumnToRecord(PrimaryKey pk, Column v) {
        Record line = new DefaultRecord();
        for (PrimaryKeyColumn pkc : pk.getPrimaryKeyColumns()) {
            line.addColumn(TranformHelper.otsPrimaryKeyColumnToDataxColumn(pkc));
        }
        line.addColumn(new StringColumn(v.getName()));
        line.addColumn(new LongColumn(v.getTimestamp()));
        line.addColumn(TranformHelper.otsColumnToDataxColumn(v));
        
        return line;
    }
}
