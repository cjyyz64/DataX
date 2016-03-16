package com.alibaba.datax.plugin.writer.hbase11xwriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by shf on 16/4/6.
 */
public class NormalTaskTest {

    @Test
    public void testConvertRecordToPut() throws Exception {
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("getrowkey"));
        record.addColumn(new StringColumn("test"));
        record.addColumn(new LongColumn("123"));
        record.addColumn(new DoubleColumn("123.123"));

        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","users");
        String column = "[{\"index\":\"1\",\"name\":\"info:age\",\"type\":\"string\"}," +
                "{\"index\":\"2\",\"name\":\"info:birthday\",\"type\":\"int\"}," +
                "{\"index\":\"3\",\"name\":\"info:company\",\"type\":\"double\"}]";
        List columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        String rowkeyColumn = "[{\"index\":\"0\",\"type\":\"string\"},{\"index\":\"-1\",\"type\":\"string\",\"value\":\"_\"}]";
        List rowkeyColumnjson = JSON.parseObject(rowkeyColumn, new TypeReference<List>() {});
        configuration.set(Key.ROWKEY_COLUMN,rowkeyColumnjson);
        configuration.set(Key.MODE,"normal");
        NormalTask normalTask = new NormalTask(configuration);

        Put put1 = normalTask.convertRecordToPut(record);

        Put put2 = new Put(Bytes.toBytes("getrowkey_"));
        put2.setDurability(Durability.SKIP_WAL);
        int  datai =123;
        double datad =123.123;
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"),Bytes.toBytes("test"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"),Bytes.toBytes(datai));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("company"),Bytes.toBytes(datad));
        System.out.println(JSON.toJSONString(put1));
        System.out.println(JSON.toJSONString(put2));
        //"$ref": "$.familyCellMap.[B@1c07c1e7[0]" 地址不同 不equal
        //assertEquals(JSON.toJSONString(put1), JSON.toJSONString(put2));
    }

    @Test
    public void testGetRowkey() throws Exception {
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("getrowkey"));
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","users");
        String column = "[{\"index\":\"1\",\"name\":\"info:age\",\"type\":\"string\"}," +
                "{\"index\":\"2\",\"name\":\"info:birthday\",\"type\":\"string\"}," +
                "{\"index\":\"3\",\"name\":\"info:company\",\"type\":\"string\"}]";
        List columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        String rowkeyColumn = "[{\"index\":\"0\",\"type\":\"string\"},{\"index\":\"-1\",\"type\":\"string\",\"value\":\"_\"}]";
        List rowkeyColumnjson = JSON.parseObject(rowkeyColumn, new TypeReference<List>() {});
        configuration.set(Key.ROWKEY_COLUMN,rowkeyColumnjson);
        configuration.set(Key.MODE,"normal");
        NormalTask normalTask = new NormalTask(configuration);
        byte[]  bytes = normalTask.getRowkey(record);
        byte[] bytes2 = Bytes.toBytes("getrowkey_");
        assertEquals(JSON.toJSONString(bytes), JSON.toJSONString(bytes2));

        //index 超出范围
        rowkeyColumn = "[{\"index\":\"1000\",\"type\":\"string\"},{\"index\":\"-1\",\"type\":\"string\",\"value\":\"_\"}]";
        rowkeyColumnjson = JSON.parseObject(rowkeyColumn, new TypeReference<List>() {});
        configuration.set(Key.ROWKEY_COLUMN,rowkeyColumnjson);
        configuration.set(Key.MODE,"normal");
        normalTask = new NormalTask(configuration);
        try {
            bytes = normalTask.getRowkey(record);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("您的rowkeyColumn配置项中中index值超出范围"));
        }

    }

    @Test
    public void testGetVersion() throws Exception {
        Record record = new DefaultRecord();
        record.addColumn(new LongColumn("1234567"));
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","users");
        String column = "[{\"index\":\"1\",\"name\":\"info:age\",\"type\":\"string\"}," +
                "{\"index\":\"2\",\"name\":\"info:birthday\",\"type\":\"string\"}," +
                "{\"index\":\"3\",\"name\":\"info:company\",\"type\":\"string\"}]";
        List columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        String rowkeyColumn = "[{\"index\":\"0\",\"type\":\"string\"},{\"index\":\"-1\",\"type\":\"string\",\"value\":\"_\"}]";
        List rowkeyColumnjson = JSON.parseObject(rowkeyColumn, new TypeReference<List>() {});
        configuration.set(Key.ROWKEY_COLUMN,rowkeyColumnjson);
        String versionColumn = "{\"index\":\"0\"}";
        Map versionColumnjson = JSON.parseObject(versionColumn, new TypeReference<Map>() {});
        configuration.set(Key.VERSION_COLUMN,versionColumnjson);
        configuration.set(Key.MODE,"normal");
        NormalTask normalTask = new NormalTask(configuration);
        long version = normalTask.getVersion(record);
        long version2  = 1234567;
        assertEquals(version, version2);

        //常量
        versionColumn = "{\"index\":\"-1\",\"value\":\"2016-04-05 12:31:32\",\"format\":\"yy-MM-dd HH:mm:ss\"}";
        versionColumnjson = JSON.parseObject(versionColumn, new TypeReference<Map>() {});
        configuration.set(Key.VERSION_COLUMN,versionColumnjson);
        normalTask = new NormalTask(configuration);
        version = normalTask.getVersion(record);
        DateFormat dateFormat = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
        version2  = dateFormat.parse("2016-04-05 12:31:32").getTime();
        assertEquals(version, version2);

        //index 超出范围
        versionColumn = "{\"index\":\"1000\"}";
        versionColumnjson = JSON.parseObject(versionColumn, new TypeReference<Map>() {});
        configuration.set(Key.VERSION_COLUMN,versionColumnjson);
        normalTask = new NormalTask(configuration);
        try {
            version = normalTask.getVersion(record);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("您的versionColumn配置项中中index值超出范围"));
        }
    }


    @Test
    public void testGetColumnByte() throws Exception {

        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","users");
        String column = "[{\"index\":\"1\",\"name\":\"info:age\",\"type\":\"string\"}," +
                "{\"index\":\"2\",\"name\":\"info:birthday\",\"type\":\"string\"}," +
                "{\"index\":\"3\",\"name\":\"info:company\",\"type\":\"string\"}]";
        List columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        String rowkeyColumn = "[{\"index\":\"0\",\"type\":\"string\"}]";
        List rowkeyColumnjson = JSON.parseObject(rowkeyColumn, new TypeReference<List>() {});
        configuration.set(Key.ROWKEY_COLUMN,rowkeyColumnjson);
        configuration.set(Key.MODE,"normal");
        NormalTask normalTask = new NormalTask(configuration);

        //默认nullmode skip
        Column datacolumn =new StringColumn(null);
        byte[] bytes = normalTask.getColumnByte(ColumnType.BOOLEAN,datacolumn);
        assertEquals(bytes,null);
        //nullmode empty
        configuration.set(Key.NULL_MODE,NullModeType.Empty);
        normalTask = new NormalTask(configuration);
        bytes = normalTask.getColumnByte(ColumnType.BOOLEAN,datacolumn);
        assertEquals(JSON.toJSONString(bytes), JSON.toJSONString(HConstants.EMPTY_BYTE_ARRAY));

        //boolean
        ColumnType columnType = ColumnType.BOOLEAN;
        datacolumn =new BoolColumn("true");
        bytes = normalTask.getColumnByte(columnType,datacolumn);
        byte[] bytes2 = Bytes.toBytes(true);
        assertEquals(JSON.toJSONString(bytes), JSON.toJSONString(bytes2));

        //short
        columnType = ColumnType.SHORT;
        datacolumn =new LongColumn(127);
        bytes = normalTask.getColumnByte(columnType,datacolumn);
        short data =127;
        bytes2 = Bytes.toBytes(data);
        assertEquals(JSON.toJSONString(bytes), JSON.toJSONString(bytes2));
        //int
        columnType = ColumnType.INT;
        datacolumn =new LongColumn(127);
        bytes = normalTask.getColumnByte(columnType,datacolumn);
        int dataint =127;
        bytes2 = Bytes.toBytes(dataint);
        assertEquals(JSON.toJSONString(bytes), JSON.toJSONString(bytes2));

        //long
        columnType = ColumnType.LONG;
        datacolumn =new LongColumn(127);
        bytes = normalTask.getColumnByte(columnType,datacolumn);
        long datalong =127l;
        bytes2 = Bytes.toBytes(datalong);
        assertEquals(JSON.toJSONString(bytes), JSON.toJSONString(bytes2));
        //float
        columnType = ColumnType.FLOAT;
        datacolumn =new DoubleColumn("127.127");
        bytes = normalTask.getColumnByte(columnType,datacolumn);
        Float dataf =127.127f;
        bytes2 = Bytes.toBytes(dataf);
        assertEquals(JSON.toJSONString(bytes), JSON.toJSONString(bytes2));

        //double
        columnType = ColumnType.DOUBLE;
        datacolumn =new DoubleColumn("127.127");
        bytes = normalTask.getColumnByte(columnType,datacolumn);
        Double datad =127.127;
        bytes2 = Bytes.toBytes(datad);
        assertEquals(JSON.toJSONString(bytes), JSON.toJSONString(bytes2));

        //string
        columnType = ColumnType.STRING;
        datacolumn =new StringColumn("127.127");
        bytes = normalTask.getColumnByte(columnType,datacolumn);
        String datas ="127.127";
        bytes2 = Bytes.toBytes(datas);
        assertEquals(JSON.toJSONString(bytes), JSON.toJSONString(bytes2));

    }

}