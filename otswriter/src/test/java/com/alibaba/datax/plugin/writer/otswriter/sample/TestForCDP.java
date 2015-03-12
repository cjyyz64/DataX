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


public class TestForCDP {
    
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
        reservedThroughput.setCapacityUnit(new CapacityUnit(5000, 5000));
        
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
    
    private void batchWriteRow(String tableName, int pkBegin, int pkEnd, List<Pair<String, ColumnValue>> values) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        for (int i = pkBegin; i < pkEnd; i++) {
            batchWriteRowRequest.addRowPutChange(getRowPutChange(tableName, i, values));
        }
        ots.batchWriteRow(batchWriteRowRequest);
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
        clear();
        Thread.sleep(1 * 1000);
        List<Integer> cu = new ArrayList<Integer>();
        cu.add(1);
        cu.add(10);
        cu.add(100);
        cu.add(1000);
        cu.add(5000);
        List<Integer> data = new ArrayList<Integer>();
        data.add(5);
        data.add(50); 
        data.add(100);
        data.add(200);
        data.add(500);

        for (int i = 0; i < cu.size(); i++) {
            for (int j = 0; j < data.size(); j++) {
                String tableName = String.format("cdp_cu%d_size%d", cu.get(i), data.get(j) * 10);
//                createTable(tableName);
//                Thread.sleep(5 * 1000);
//                prepareData(tableName, 1000000, data.get(j));// v * 10 
                //Thread.sleep(2 * 1000);
                //updateTable(tableName, cu.get(i));
            }
        }
    }
}
