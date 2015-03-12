package com.alibaba.datax.plugin.writer.otswriter.sample;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.otswriter.common.BaseTest;
import com.alibaba.datax.plugin.writer.otswriter.common.Utils;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSBatchWriterRowTask;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.utils.DefaultNoRetry;
import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.OTSClientAsync;
import com.aliyun.openservices.ots.OTSServiceConfiguration;
import com.aliyun.openservices.ots.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.RowPutChange;

public class TestBatchWriteRow {

    public static String tableName = "ots_datax_perf_new_1";
    
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        
        Configuration p = Utils.loadConf();
        
        ClientConfiguration clientConfigure = new ClientConfiguration();
        clientConfigure.setIoThreadCount(Integer.parseInt(args[0]));
        clientConfigure.setMaxConnections(Integer.parseInt(args[1]));
        
        System.out.println("IO Thread: "+ args[0] +", MaxConnetion:" + args[1]);
        
        OTSServiceConfiguration otsConfigure = new OTSServiceConfiguration();
        otsConfigure.setRetryStrategy(new DefaultNoRetry());
        
        OTSClientAsync ots = new OTSClientAsync(
                p.getString("endpoint"), 
                p.getString("accessid"), 
                p.getString("accesskey"), 
                p.getString("instance-name"),
                clientConfigure,
                otsConfigure,
                null);
        
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        
        Record record = new DefaultRecord();
        OTSLine line = new OTSLine(tableName, OTSOpType.PUT_ROW, record, null, null);
        
        for (int i = 0; i < 100; i++) {
            RowPrimaryKey primaryKey = new RowPrimaryKey();
            primaryKey.addPrimaryKeyColumn("userid", PrimaryKeyValue.fromString(String.valueOf(i)));
            primaryKey.addPrimaryKeyColumn("groupid", PrimaryKeyValue.fromLong(i));
            
            RowPutChange rowPutChange = new RowPutChange(tableName);
            rowPutChange.setPrimaryKey(primaryKey);
            
            rowPutChange.addAttributeColumn("string_0", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_1", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_2", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_3", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_4", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_5", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_6", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_7", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_8", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_9", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_10", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_11", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_12", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_13", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("string_14", ColumnValue.fromString("0123456789"));
            rowPutChange.addAttributeColumn("int_0", ColumnValue.fromLong(0));
            rowPutChange.addAttributeColumn("int_0", ColumnValue.fromLong(0));
            
            batchWriteRowRequest.addRowPutChange(rowPutChange);
        }

        for (int i = 0; i < 9999999;i++)
        {
            //new OTSBatchWriterRowTask(null, null, null, ots, null, null, 0);
            //ots.batchWriteRow(batchWriteRowRequest);
        }
        ots.shutdown();
    }
}
