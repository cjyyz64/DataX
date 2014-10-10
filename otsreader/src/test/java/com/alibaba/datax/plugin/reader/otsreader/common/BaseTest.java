package com.alibaba.datax.plugin.reader.otsreader.common;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

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
    
    public void prepareData(List<PrimaryKeyType> pkType, int rowCount, double nullPercent) {
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
        
        Table t = new Table(ots, tableName, pkType, attriTypes, nullPercent);
        t.create();
        t.insertData(rowCount);
    }
    
    public OTSConf getConf (
            int pkCount, 
            int attrCount, 
            int constTimes, 
            List<PrimaryKeyValue> rangeBegin, 
            List<PrimaryKeyValue> rangeEnd,
            List<PrimaryKeyValue> rangeSplit
            ) {
        OTSConf conf = new OTSConf();
        conf.setEndpoint(p.getString("endpoint"));
        conf.setAccessId(p.getString("accessid"));
        conf.setAccesskey(p.getString("accesskey"));
        conf.setInstanceName(p.getString("instance-name"));
        conf.setTableName(tableName);
        
        Person person = new Person();
        person.setName("为硬音k，近似普通话轻声以外的g: cum,cīvis,facilis");
        person.setAge(Long.MAX_VALUE);
        person.setHeight(1111);
        person.setMale(false);

        List<OTSColumn> columns = new ArrayList<OTSColumn>();

        for (int i = 0; i < pkCount; i++) {
            columns.add(OTSColumn.fromNormalColumn("pk_" + i));
        }

        for (int i = 0; i < attrCount; i++) {
            columns.add(OTSColumn.fromNormalColumn("attr_" + i));
        }

        for (int i = 0; i < constTimes; i++) {
            columns.add(OTSColumn.fromConstStringColumn(""));
            columns.add(OTSColumn.fromConstStringColumn("测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^"));
            columns.add(OTSColumn.fromConstIntegerColumn(100L));
            columns.add(OTSColumn.fromConstDoubleColumn(1121111111111.0));
            columns.add(OTSColumn.fromConstIntegerColumn(Long.MIN_VALUE));
            columns.add(OTSColumn.fromConstIntegerColumn(Long.MAX_VALUE));
            columns.add(OTSColumn.fromConstBoolColumn(true));
            columns.add(OTSColumn.fromConstBoolColumn(false));
            columns.add(OTSColumn.fromConstBytesColumn(Person.toByte(person)));
        }
        conf.setColumns(columns);
        
        conf.setRangeBegin(rangeBegin);
        conf.setRangeEnd(rangeEnd);
        conf.setRangeSplit(rangeSplit);

        return conf;
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
