package com.alibaba.datax.plugin.writer.hbase11xwriter;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by shf on 16/3/17.
 */
public class Hbase11xTest {

    public  void insterRow(Connection hConnection, String tableName, String rowkey, String colFamily, String col, String val) throws IOException {
        Table table = hConnection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);

        //批量插入
       /* List<Put> putList = new ArrayList<Put>();
        puts.add(put);
        table.put(putList);*/
        table.close();
        hConnection.close();
    }

    @Test
    public void testGetHbaseConnection() throws Exception {
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        org.apache.hadoop.hbase.client.Connection hConnection= Hbase11xHelper.getHbaseConnection(hbaseConfig);
        assertNotNull(hConnection);
        String tableName = "writer";
        String rowkey = "lisi";
        String colFamily = "cf1";
        String col= "q1";
        String val = "25";
        insterRow(hConnection, tableName,  rowkey,  colFamily,  col,  val);

    }

    @Test
    public void testGetTable() throws Exception {

    }


}