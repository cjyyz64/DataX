package com.alibaba.datax.plugin.writer.otswriter.common;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.PrimaryKeyType;

public class BaseTest{
    private String tableName;
    private Configuration p = Utils.loadConf();
    private OTSClient ots = new OTSClient(
            p.getString("endpoint"), 
            p.getString("accessid"), 
            p.getString("accesskey"), 
            p.getString("instance-name"));


    public BaseTest (String tableName) {
        this.tableName = tableName;
    }
    
    public void close() {
        ots.shutdown();
    }
    
    public void createTable(List<PrimaryKeyType> pkType, int readCapacityUnit, int writeCapacityUnit) {
        Table t = new Table(ots, tableName, pkType);
        t.create(readCapacityUnit, writeCapacityUnit);
    }
    
    public void createTable(List<PrimaryKeyType> pkType) {
        createTable(pkType, 5000, 5000);
    }
    
    public void prepareData(List<PrimaryKeyType> pkType, List<ColumnType> attrTypes, long begin, long rowCount, double nullPercent) {
        Table t = new Table(ots, tableName, pkType, attrTypes, nullPercent);
        t.create(5000, 5000);
        t.insertData(begin, rowCount);
    }
    
    public void prepareData(List<PrimaryKeyType> pkType, long begin, long rowCount, double nullPercent) {
        List<ColumnType> attriTypes = new ArrayList<ColumnType>();
        attriTypes.add(ColumnType.STRING);
        attriTypes.add(ColumnType.INTEGER);
        attriTypes.add(ColumnType.DOUBLE);
        attriTypes.add(ColumnType.BOOLEAN);
        attriTypes.add(ColumnType.BINARY);
        attriTypes.add(ColumnType.STRING);
        attriTypes.add(ColumnType.INTEGER);
        attriTypes.add(ColumnType.DOUBLE);
        attriTypes.add(ColumnType.BOOLEAN);
        attriTypes.add(ColumnType.BINARY);
        
        prepareData(pkType, attriTypes, begin, rowCount, nullPercent);
    }
    
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Configuration getP() {
        return p;
    }

    public void setP(Configuration p) {
        this.p = p;
    }

    public OTSClient getOts() {
        return ots;
    }

    public void setOts(OTSClient ots) {
        this.ots = ots;
    }

}
