package com.alibaba.datax.plugin.writer.otswriter.sample;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.common.Utils;
import com.alibaba.datax.plugin.writer.otswriter.model.Pair;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.model.BatchWriteRowResult;
import com.aliyun.openservices.ots.model.BatchWriteRowResult.RowStatus;
import com.aliyun.openservices.ots.model.CapacityUnit;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.CreateTableRequest;
import com.aliyun.openservices.ots.model.DeleteTableRequest;
import com.aliyun.openservices.ots.model.ListTableResult;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.ReservedThroughput;
import com.aliyun.openservices.ots.model.ReservedThroughputChange;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.RowPutChange;
import com.aliyun.openservices.ots.model.TableMeta;
import com.aliyun.openservices.ots.model.UpdateTableRequest;


public class TestForCDPNew {
    
    private static Configuration p = Utils.loadConf();
    private static OTSClient ots = new OTSClient(
            p.getString("endpoint"), 
            p.getString("accessid"), 
            p.getString("accesskey"), 
            p.getString("instance-name"));
    
    @BeforeClass
    public static void setup() {
        
    }
    
    @AfterClass
    public static void teardown() {
        ots.shutdown();
    }
    
    private void clear() {
        ListTableResult result = ots.listTable();
        for (String tableName : result.getTableNames()) {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
            deleteTableRequest.setTableName(tableName);
            System.out.println("TableName : " + tableName);
            //ots.deleteTable(deleteTableRequest);
        }
    }
    
    private void createTable(String tableName) {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.INTEGER);
        
        
        ReservedThroughput reservedThroughput = new ReservedThroughput();
        reservedThroughput.setCapacityUnit(new CapacityUnit(1, 1));
        
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableMeta(tableMeta);
        createTableRequest.setReservedThroughput(reservedThroughput);
        ots.createTable(createTableRequest);
    }
    
    private RowPutChange getRowPutChange(String tableName, int pkValue, List<Pair<String, ColumnValue>> values) {
        RowPrimaryKey pk = new RowPrimaryKey();
        pk.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(pkValue));
        RowPutChange rowPutChange = new RowPutChange(tableName);
        rowPutChange.setPrimaryKey(pk);
        for (Pair<String, ColumnValue> en : values) {
            rowPutChange.addAttributeColumn(en.getKey(), en.getValue());
        }
        return rowPutChange;
    }
    
    private BatchWriteRowResult batchWriteRow(String tableName, int pkBegin, int pkEnd, List<Pair<String, ColumnValue>> values) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        for (int i = pkBegin; i < pkEnd; i++) {
            batchWriteRowRequest.addRowPutChange(getRowPutChange(tableName, i, values));
        }
        return ots.batchWriteRow(batchWriteRowRequest);
    }
    
    private void prepareData(String tableName, int count, int cellSize) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cellSize; i++) {
            sb.append('A');
        }
        String value = sb.toString();
        List<Pair<String, ColumnValue>> values = new ArrayList<Pair<String, ColumnValue>>();
        for (int i = 0; i < 10; i++) {
            values.add(new Pair<String, ColumnValue>(String.format("attr_%04d", i), ColumnValue.fromString(value)));
        }
        for (int i = 0; i < count; i += 10) {
            System.out.println(String.format("%s input : (%d%%), %d", tableName, (int)(1.0 * i / count * 100), i));
            batchWriteRow(tableName, i, i + 10, values);
        }
    }
    
    private void updateTable(String tableName, int cu) {
        ReservedThroughputChange reservedThroughput = new ReservedThroughputChange();
        reservedThroughput.setReadCapacityUnit(cu);
        reservedThroughput.setWriteCapacityUnit(cu);
        
        UpdateTableRequest updateTableRequest = new UpdateTableRequest();
        updateTableRequest.setTableName(tableName);
        updateTableRequest.setReservedThroughputChange(reservedThroughput);
        ots.updateTable(updateTableRequest);
    }

    @Test
    public void test() throws Exception {
        String tableName = "tt_cu1";
        //createTable(tableName);
        //Thread.sleep(5000);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            sb.append('A');
        }
        List<Pair<String, ColumnValue>> values = new ArrayList<Pair<String, ColumnValue>>();
        for (int i = 0; i < 10; i++) {
            values.add(new Pair<String, ColumnValue>(String.format("attr_%04d", i), ColumnValue.fromString(sb.toString())));
        }
        BatchWriteRowResult r = batchWriteRow(tableName, 0, 2, values);
        for (RowStatus s : r.getPutRowStatus(tableName)) {
            if (s.getError() == null) {
                System.out.println(String.format("SUCC, Read CU: %d, Write CU: %d", s.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit(), s.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit()));
            } else {
                System.out.println(String.format("ERRO, Code: %s", s.getError().getCode()));
            }
        }
        
    }
}
