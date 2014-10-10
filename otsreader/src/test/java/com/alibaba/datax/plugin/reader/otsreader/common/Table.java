package com.alibaba.datax.plugin.reader.otsreader.common;

import java.util.List;

import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.CapacityUnit;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.CreateTableRequest;
import com.aliyun.openservices.ots.model.DeleteTableRequest;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.PutRowRequest;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.RowPutChange;
import com.aliyun.openservices.ots.model.TableMeta;

public class Table {
    private String tableName = null;
    private double nullPercent = 0.0;
    
    private OTSClient ots = null;
    private List<PrimaryKeyType> pkTypes;
    private List<ColumnType> attriTypes;

    public Table(OTSClient ots, String tableName, List<PrimaryKeyType> pkType, List<ColumnType> attriTypes, double nullPercent) {
        this.ots = ots;
        this.nullPercent = nullPercent;
        this.tableName = tableName;
        this.pkTypes = pkType;
        this.attriTypes = attriTypes;
    }
    
    public void create() {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setTableName(tableName);
        try {
            ots.deleteTable(deleteTableRequest);
        } catch (Exception e) {
            //e.printStackTrace();
        }
        
        TableMeta meta =  new TableMeta(this.tableName);
        for (int i = 0; i < this.pkTypes.size(); i++) {
            String name = String.format("pk_%d", i);
            meta.addPrimaryKeyColumn(name, pkTypes.get(i));
        }
        CapacityUnit capacityUnit = new CapacityUnit(5000, 5000);
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableMeta(meta);
        createTableRequest.setReservedThroughput(capacityUnit);
        try {
            ots.createTable(createTableRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public void insertData(long rowCount) {
        for (long i = 0; i < rowCount; i++) {
            RowPutChange rowChange = new RowPutChange(tableName);
            RowPrimaryKey primaryKey = new RowPrimaryKey();
            for (int j = 0; j < this.pkTypes.size(); j++) {
                String name = String.format("pk_%d", j);
                PrimaryKeyType type = pkTypes.get(j);
                if (type == PrimaryKeyType.INTEGER) {
                    primaryKey.addPrimaryKeyColumn(name, PrimaryKeyValue.fromLong(i));
                } else {
                    primaryKey.addPrimaryKeyColumn(name, PrimaryKeyValue.fromString(String.format("%d", i)));
                }
            }
            
            rowChange.setPrimaryKey(primaryKey);
            
            for (int j = 0; j < this.attriTypes.size(); j++) {
                String name = String.format("attr_%d", j);
                ColumnType type = attriTypes.get(j);
                
                switch(type) {
                case INTEGER: double r0 = Math.random(); if (r0 >= nullPercent){rowChange.addAttributeColumn(name, ColumnValue.fromLong(i));} break;
                case DOUBLE: double r1 = Math.random(); if (r1 >= nullPercent){rowChange.addAttributeColumn(name, ColumnValue.fromDouble(i));} break;
                case STRING: double r2 = Math.random(); if (r2 >= nullPercent){rowChange.addAttributeColumn(name, ColumnValue.fromString(String.format("%d", i)));} break;
                case BOOLEAN: double r3 = Math.random(); if (r3 >= nullPercent){rowChange.addAttributeColumn(name, ColumnValue.fromBoolean(i % 2 == 0 ? true : false));} break;
                case BINARY: double r4 = Math.random(); if (r4 >= nullPercent){rowChange.addAttributeColumn(name, ColumnValue.fromBinary(String.format("%d", i).getBytes()));} break;
                }
            }
            
            PutRowRequest putRowRequest = new PutRowRequest();
            putRowRequest.setRowChange(rowChange);
            ots.putRow(putRowRequest);
        }
    }
    
    public void close() {
        ots.shutdown();
    }
}
