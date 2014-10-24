package com.alibaba.datax.plugin.writer.otswriter.sample;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.plugin.writer.otswriter.common.BaseTest;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.Condition;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.RowPutChange;
import com.aliyun.openservices.ots.model.RowUpdateChange;

public class TestBatchWriteRow {

    public static String tableName = "ots_writer_test_batch_write";
    public static BaseTest base = new BaseTest(tableName);
    
    public static void prepare() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        
        base.prepareData(pk, -100, 0, 0.5);
    }

    public static void main(String[] args) {
        // TODO Auto-generated method stub

        TestBatchWriteRow.prepare();
        
        OTSClient ots = TestBatchWriteRow.base.getOts();

        System.out.println("0");
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        RowPrimaryKey primaryKey = new RowPrimaryKey();
        primaryKey.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello"));
        primaryKey.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(1000));

        RowPutChange rowPutChange = new RowPutChange(tableName);
        rowPutChange.setCondition(new Condition());
        rowPutChange.setPrimaryKey(primaryKey);
        //rowPutChange.addAttributeColumn("attr_0", ColumnValue.fromLong(100));
        //rowPutChange.addAttributeColumn("attr_1", ColumnValue.fromString("big"));
        batchWriteRowRequest.addRowPutChange(rowPutChange);
        System.out.println("1");
        ots.batchWriteRow(batchWriteRowRequest);
        System.out.println("2");
        
        RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName);
        rowUpdateChange.setCondition(new Condition());
        rowUpdateChange.setPrimaryKey(primaryKey);
        rowPutChange.addAttributeColumn("attr_0", ColumnValue.fromLong(100));
        rowPutChange.addAttributeColumn("attr_1", ColumnValue.fromString("big"));
        batchWriteRowRequest.addRowUpdateChange(rowUpdateChange);
        System.out.println("1");
        ots.batchWriteRow(batchWriteRowRequest);
        System.out.println("2");
        ots.shutdown();
    }
}
