package com.alibaba.datax.plugin.writer.otswriter.utils;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.RowPutChange;
import com.aliyun.openservices.ots.model.RowUpdateChange;

public class Common {

    public static String getDetailMessage(Exception exception) {
        if (exception instanceof OTSException) {
            OTSException e = (OTSException) exception;
            return "OTSException[ErrorCode:" + e.getErrorCode() + ", ErrorMessage:" + e.getMessage() + ", RequestId:" + e.getRequestId() + "]";
        } else if (exception instanceof ClientException) {
            ClientException e = (ClientException) exception;
            return "ClientException[ErrorCode:" + e.getErrorCode() + ", ErrorMessage:" + e.getMessage() + "]";
        } else if (exception instanceof IllegalArgumentException) {
            IllegalArgumentException e = (IllegalArgumentException) exception;
            return "IllegalArgumentException[ErrorMessage:" + e.getMessage() + "]";
        } else {
            return "Exception[ErrorMessage:" + exception.getMessage() + "]";
        }
    }
    
    private static RowPrimaryKey getPKFromRecord(OTSConf conf, Record r) {
        RowPrimaryKey primaryKey = new RowPrimaryKey();
        int pkCount = conf.getPrimaryKeyColumn().size();
        for (int i = 0; i < pkCount; i++) {
            PrimaryKeyValue pk = ColumnConversion.columnToPrimaryKeyValue(r.getColumn(i), conf.getPrimaryKeyColumn().get(i));
            primaryKey.addPrimaryKeyColumn(conf.getPrimaryKeyColumn().get(i).getName(), pk);
        }
        return primaryKey;
    }
    
    public static RowPutChange recordToRowPutChange(OTSConf conf, Record r) {
        RowPutChange rowPutChange = new RowPutChange(conf.getTableName());
        rowPutChange.setPrimaryKey(getPKFromRecord(conf, r));
        for (int i = 0; i < conf.getAttributeColumn().size(); i++) {
            ColumnValue cv = ColumnConversion.columnToColumnValue(
                    r.getColumn(i + conf.getPrimaryKeyColumn().size()), 
                    conf.getAttributeColumn().get(i));
            
            rowPutChange.addAttributeColumn(conf.getAttributeColumn().get(i).getName(), cv);
        }
        return rowPutChange;
    }
    
    public static RowUpdateChange recordToRowUpdateChange(OTSConf conf, Record r) {
        RowUpdateChange rowUpdateChange = new RowUpdateChange(conf.getTableName());
        rowUpdateChange.setPrimaryKey(getPKFromRecord(conf, r));
        for (int i = 0; i < conf.getAttributeColumn().size(); i++) {
            ColumnValue cv = ColumnConversion.columnToColumnValue(
                    r.getColumn(i + conf.getPrimaryKeyColumn().size()), 
                    conf.getAttributeColumn().get(i));
            rowUpdateChange.addAttributeColumn(conf.getAttributeColumn().get(i).getName(), cv);
        }
        return rowUpdateChange;
    }
}
