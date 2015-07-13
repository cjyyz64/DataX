package com.alibaba.datax.plugin.reader.otsreader.smaple;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.alibaba.datax.plugin.reader.otsreader.common.OtsHelper;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.ColumnValue;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.internal.model.PutRowRequest;
import com.aliyun.openservices.ots.internal.model.RowPutChange;
import com.aliyun.openservices.ots.internal.model.TableMeta;

public class SmokeTest1 {
    
    private void prepareData() {
        String tableName = "SmokeTest1";
        OTS ots = OtsHelper.getOTSInstance();

        // create table
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("Uid", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("Pid", PrimaryKeyType.INTEGER);
        OtsHelper.createTableSafe(ots, tableMeta);
        
        // prepare data
        {
            List<PrimaryKeyColumn> primaryKey = new ArrayList<PrimaryKeyColumn>();
            primaryKey.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("uid_1")));
            primaryKey.add(new PrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(1)));
            
            PrimaryKey pk = new PrimaryKey(primaryKey);
            RowPutChange rowChange = new RowPutChange(tableName, pk);
            
            long ts = 10000;
            for (int i = 0; i < 5; i++) {
                rowChange.addColumn("attr", ColumnValue.fromString("" + i), ts++);
                rowChange.addColumn("attr", ColumnValue.fromLong(i), ts++);
            }
            ots.putRow(new PutRowRequest(rowChange));
        }
        {
            List<PrimaryKeyColumn> primaryKey = new ArrayList<PrimaryKeyColumn>();
            primaryKey.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("uid_2")));
            primaryKey.add(new PrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(2)));
            
            PrimaryKey pk = new PrimaryKey(primaryKey);
            RowPutChange rowChange = new RowPutChange(tableName, pk);
            
            long ts = 10000;
            for (int i = 0; i < 5; i++) {
                rowChange.addColumn("attr", ColumnValue.fromString("" + i), ts++);
                rowChange.addColumn("attr", ColumnValue.fromLong(i), ts++);
            }
            ots.putRow(new PutRowRequest(rowChange));
        }
    }
    
    @Test
    public void test() {
        prepareData();
    }
}
