package com.alibaba.datax.plugin.reader.hbase11xreader;

import com.alibaba.datax.plugin.reader.hbase11xreader.Hbase11xHelper;
import junit.framework.Assert;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by shf on 16/3/15.
 */
public class Hbase11xTest {

//    private static Configuration hConfiguration;
//    private static Connection hConnection;
//    public static Admin admin;
//    static {
//        hConfiguration = HBaseConfiguration.create();
//        hConfiguration.set("hbase.rootdir","hdfs://10.101.85.161:9000/hbase");
//        hConfiguration.set("hbase.cluster.distributed","true");
//        hConfiguration.set("hbase.zookeeper.quorum","v101085161.sqa.zmf");
//        //hConfiguration.set("hbase.master", "10.101.85.14:16000");
//        //hConfiguration.set("hbase.zookeeper.quorum", "master");
//        //
//        //hConfiguration.set("hbase.zookeeper.property.clientPort","2181");
//        //hConfiguration.set("zookeeper.znode.parent","/hbase");
//        try {
//            hConnection = ConnectionFactory.createConnection(hConfiguration);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
    // 创建数据库表
//    public static void createTable(String tableName, String[] columnFamilys)
//            throws Exception {
//        // 新建一个数据库管理员
//        HBaseAdmin hAdmin = new HBaseAdmin(hConfiguration);
//
//
//        if (hAdmin.tableExists(TableName.valueOf(tableName))) {
//            System.out.println("表已经存在");
//            System.exit(0);
//        } else {
//            // 新建一个 scores 表的描述
//            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
//            // 在描述里添加列族
//            for (String columnFamily : columnFamilys) {
//                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
//            }
//            // 根据配置好的描述建表
//            hAdmin.createTable(tableDesc);
//            System.out.println("创建表成功");
//        }
//        hAdmin.close();
//    }
//
    public static void scanData(Connection hConnection,String tableName,String startRow,String stopRow)throws IOException{
        System.out.println("查数据");
        Table table = hConnection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //scan.setStartRow(Bytes.toBytes(startRow));
        //scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            System.out.println("qqqqqqqqqqqqqqqqq:"+new String(result.getRow()));
            for(Cell cell:result.rawCells()){
                System.out.println("RowName:"+new String(CellUtil.cloneRow( cell))+" ");
                System.out.println("Timetamp:"+cell.getTimestamp()+" ");
                System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
                System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
                System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
            }
        }
        table.close();
        System.out.println("完毕");
    }
//
//    public static void scanMultiVersionData(String tableName,String startRow,String stopRow)throws IOException{
//        System.out.println("查数据");
//        Table table = hConnection.getTable(TableName.valueOf(tableName));
//        Scan scan = new Scan();
//        int maxVersion =4;
//        scan.setMaxVersions(maxVersion);
//        //scan.setStartRow(Bytes.toBytes(startRow));
//        //scan.setStopRow(Bytes.toBytes(stopRow));
//        ResultScanner resultScanner = table.getScanner(scan);
//
//        for(Result result : resultScanner){
//            System.out.println("scanMultiVersionData:"+new String(result.getRow()));
//            for(Cell cell:result.rawCells()){
//                System.out.println("RowName:"+new String(CellUtil.cloneRow( cell))+" ");
//                System.out.println("Timetamp:"+cell.getTimestamp()+" ");
//                System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
//                System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
//                System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
//            }
//        }
//        table.close();
//        System.out.println("完毕");
//    }


    @Test
    public void testGetHbaseConnection() throws Exception {
//        String hbaseConfig= "{\"hbase.zookeeper.quorum\":\"10.98.108.25,10.98.108.26,10.98.108.27\"," +
//                "\"zookeeper.znode.parent\":\"/hbase-perf\",\"hbase.client.retries.number\":\"3\"," +
//                "\"hbase.rpc.timeout\":\"60000\"," +
//                "\"hbase.cluster.switch.support\":\"true\"}";
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        org.apache.hadoop.hbase.client.Connection hConnection= Hbase11xHelper.getHbaseConnection(hbaseConfig);
        assertNotNull(hConnection);
        String tableName = "users";
        scanData(hConnection,tableName,null,null);


//        String tableName = "users";
//        System.out.println("$$$$$$$$$$$$$$$$$$$$$$");
//
//        try {
//            //createTable(tableName,null);
//            scanData(tableName,null,null);
//            scanMultiVersionData(tableName,null,null);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    @Test
    public void testCheckHbaseTable() throws Exception {
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        org.apache.hadoop.hbase.client.Connection hConnection= Hbase11xHelper.getHbaseConnection(hbaseConfig);
        Admin admin = hConnection.getAdmin();
       boolean  result = admin.isTableAvailable(TableName.valueOf("users"));
        System.out.println(result);
        assertTrue(result);
        Hbase11xHelper.checkHbaseTable(admin, TableName.valueOf("users"));
    }


    @Test
    public void testConvertUserEndRowkey() throws Exception {

    }

    @Test
    public void testValidateParameter() throws Exception {

    }


}