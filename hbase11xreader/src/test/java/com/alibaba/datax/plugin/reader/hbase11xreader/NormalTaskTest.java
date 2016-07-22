package com.alibaba.datax.plugin.reader.hbase11xreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by shf on 16/3/31.
 *
 */
public class NormalTaskTest {

    @Test
    public void testInitScan() throws Exception {
        //init method
    }

    @Test
    public void testGetNextHbaseRow() throws Exception {
        RecordSender recordSender = mock(RecordSender.class);
        when(recordSender.createRecord()).thenReturn( new DefaultRecord());
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","users");
        String column = "[{\"name\":\"rowkey\",\"type\":\"string\"}]";
        List columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.set(Key.MODE,"normal");

        NormalTask normalTask = new NormalTask(configuration);
        normalTask.prepare();
        Result result = normalTask.getNextHbaseRow();
        int i = 0;
        while (result!= null){
            System.out.println(new String(result.getRow()));
            System.out.println(new Date());
            Thread.sleep(2000);
            System.out.println(new Date());
            result = normalTask.getNextHbaseRow();
            i++;
        }
        System.out.println(i);
        assertEquals(i,2);
    }

    @Test
    public void testFetchLine() throws Exception {
        RecordSender recordSender = mock(RecordSender.class);
        when(recordSender.createRecord()).thenReturn( new DefaultRecord());
        Record record = recordSender.createRecord();
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","users");
        String column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"info:age\",\"type\":\"string\"}," +
                "{\"name\":\"info:birthday\",\"type\":\"date\",\"format\":\"yy-MM-dd\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        List columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.set(Key.MODE,"normal");

        NormalTask normalTask = new NormalTask(configuration);
        normalTask.prepare();
        normalTask.fetchLine(record);

        DateFormat dateFormat = new SimpleDateFormat("yy-MM-dd");
        Date datadate = dateFormat.parse("1987-06-17");
        Record record2 = new DefaultRecord();
        record2.addColumn(new StringColumn("lisi"));
        record2.addColumn(new StringColumn("30"));
        record2.addColumn(new DateColumn(datadate));
        record2.addColumn(new StringColumn("qiran"));

        for(int i =0;i<record.getColumnNumber();i++){
            System.out.println(JSON.toJSONString(record.getColumn(i)));
            System.out.println(JSON.toJSONString(record2.getColumn(i)));
            assertEquals(JSON.toJSONString(record.getColumn(i)),JSON.toJSONString(record2.getColumn(i)));
        }
    }


    @Test
    public void testPrepare() throws Exception {
        //init method
    }

    @Test
    public void testClose() throws Exception {
        //close method
    }


    @Test
    public void testConvertBytesToAssignType() throws Exception {
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","users");
        String column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"info:age\",\"type\":\"string\"}," +
                "{\"name\":\"info:birthday\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        List columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.set(Key.MODE,"normal");

        NormalTask normalTask = new NormalTask(configuration);
        //boolean null
        ColumnType columnType = ColumnType.BOOLEAN;
        byte[] byteArray;
        String dateformat;
        Column convertColumn = normalTask.convertBytesToAssignType(columnType, null, null);
        Column convertColumn2 = new BoolColumn((Boolean)null);
        System.out.println("boolean null:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));
        //boolean
        byteArray = Bytes.toBytes(true);
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new BoolColumn(true);
        System.out.println("boolean:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));
        //boolean byteArray 为空
        byteArray = new byte[0];
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new BoolColumn((Boolean)null);
        System.out.println("boolean byte[0]:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        //short null
        columnType = ColumnType.SHORT;
        convertColumn = normalTask.convertBytesToAssignType(columnType, null, null);
        convertColumn2 = new LongColumn((String) null);
        System.out.println("short null:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));
        //short
        Short data = 127;
        byteArray = Bytes.toBytes(data);
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new LongColumn(data.toString());
        System.out.println("short:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        byteArray = new byte[0];
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new LongColumn((String) null);
        System.out.println("short byte[0]:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        //int null
        columnType = ColumnType.INT;
        convertColumn = normalTask.convertBytesToAssignType(columnType, null, null);
        convertColumn2 = new LongColumn((Integer) null);
        System.out.println("int null:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));
        //int
        int dataint = 127;
        byteArray = Bytes.toBytes(dataint);
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new LongColumn(127);
        System.out.println("int:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        byteArray = new byte[0];
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new LongColumn((String) null);
        System.out.println("int byte[0]:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        //long
        columnType = ColumnType.LONG;
        convertColumn = normalTask.convertBytesToAssignType(columnType, null, null);
        convertColumn2 = new LongColumn((Long) null);
        System.out.println("long null:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        Long datalong = 127L;
        byteArray = Bytes.toBytes(datalong);
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new LongColumn(127);
        System.out.println("long:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        byteArray = new byte[0];
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new LongColumn((String) null);
        System.out.println("long byte[0]:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        //float
        columnType = ColumnType.FLOAT;
        convertColumn = normalTask.convertBytesToAssignType(columnType, null, null);
        convertColumn2 = new DoubleColumn((Float) null);
        System.out.println("float null:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        float datafloat = 127.127f;
        byteArray = Bytes.toBytes(datafloat);
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new DoubleColumn(datafloat);
        System.out.println("float :");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        byteArray = new byte[0];
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new DoubleColumn((Float) null);
        System.out.println("float byte[0]:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        //double
        columnType = ColumnType.DOUBLE;
        convertColumn = normalTask.convertBytesToAssignType(columnType, null, null);
        convertColumn2 = new DoubleColumn((Double) null);
        System.out.println("double null:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        Double datadouble = 127.127;
        byteArray = Bytes.toBytes(datadouble);
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new DoubleColumn(datafloat);
        System.out.println("double null:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        byteArray = new byte[0];
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new DoubleColumn((Float) null);
        System.out.println("double byte[0]:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        //STRING
        columnType = ColumnType.STRING;
        convertColumn = normalTask.convertBytesToAssignType(columnType, null, null);
        convertColumn2 = new StringColumn(null);
        System.out.println("string null:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        String datastring = "127.127";
        byteArray = Bytes.toBytes(datastring);
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new StringColumn(new String(byteArray, "utf-8"));
        System.out.println("string:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        byteArray = new byte[0];
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new StringColumn(null);
        System.out.println("string byte[0]:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        //BINARY_STRING
        columnType = ColumnType.BINARY_STRING;
        convertColumn = normalTask.convertBytesToAssignType(columnType, null, null);
        convertColumn2 = new StringColumn(null);
        System.out.println("BINARY_STRING null:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        String databinary = "127.127";
        byteArray = Bytes.toBytes(databinary);
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new StringColumn( Bytes.toStringBinary(byteArray));
        System.out.println("BINARY_STRING:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        byteArray = new byte[0];
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new StringColumn(null);
        System.out.println("BINARY_STRING byte[0]:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        //DATE
        columnType = ColumnType.DATE;
        convertColumn = normalTask.convertBytesToAssignType(columnType, null, null);
        convertColumn2 = new DateColumn((Date) null);
        System.out.println("date null:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        DateFormat dateFormat = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
        Date date = new Date();
        String datadate = dateFormat.format(date);
        byteArray = Bytes.toBytes(datadate);
        dateformat = "yy-MM-dd HH:mm:ss";
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, dateformat);
        convertColumn2 = new DateColumn(dateFormat.parse(datadate));
        System.out.println("date:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        byteArray = new byte[0];
        convertColumn = normalTask.convertBytesToAssignType(columnType, byteArray, null);
        convertColumn2 = new DateColumn((Date) null);
        System.out.println("date byte[0]:");
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));
    }

    @Test
    public void testConvertValueToAssignType() throws Exception {

        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","users");
        String column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"info:age\",\"type\":\"string\"}," +
                "{\"name\":\"info:birthday\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        List columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.set(Key.MODE,"normal");

        NormalTask normalTask = new NormalTask(configuration);

        String value = "true";
        //boolean
        ColumnType columnType = ColumnType.BOOLEAN;
        Column convertColumn = normalTask.convertValueToAssignType(columnType,value,null);
        Column convertColumn2 = new BoolColumn("true");
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));
        //short int  long
        value = "127";
         columnType = ColumnType.SHORT;
        convertColumn = normalTask.convertValueToAssignType(columnType,value,null);
        convertColumn2 = new LongColumn(value);
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));
        columnType = ColumnType.INT;
        convertColumn = normalTask.convertValueToAssignType(columnType,value,null);
        convertColumn2 = new LongColumn(value);
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));
        columnType = ColumnType.LONG;
        convertColumn = normalTask.convertValueToAssignType(columnType,value,null);
        convertColumn2 = new LongColumn(value);
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        //FLOAT DOUBLE
        value = "127.127";
        columnType = ColumnType.FLOAT;
        convertColumn = normalTask.convertValueToAssignType(columnType,value,null);
        convertColumn2 = new DoubleColumn(value);
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));
        columnType = ColumnType.DOUBLE;
        convertColumn = normalTask.convertValueToAssignType(columnType,value,null);
        convertColumn2 = new DoubleColumn(value);
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));
        //STRING
        columnType = ColumnType.STRING;
        convertColumn = normalTask.convertValueToAssignType(columnType,value,null);
        convertColumn2 = new StringColumn(value);
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        //DATE
        columnType = ColumnType.DATE;
        DateFormat dateFormat = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
        Date date = new Date();
        String datadate = dateFormat.format(date);
        convertColumn = normalTask.convertValueToAssignType(columnType,datadate,"yy-MM-dd HH:mm:ss");
        convertColumn2 = new DateColumn(dateFormat.parse(datadate));
        System.out.println(JSON.toJSONString(convertColumn));
        System.out.println(JSON.toJSONString(convertColumn2));
        assertEquals(JSON.toJSONString(convertColumn),JSON.toJSONString(convertColumn2));

        //binary_string
        columnType = ColumnType.BINARY_STRING;
        try {
            normalTask.convertValueToAssignType(columnType,datadate,"yy-MM-dd HH:mm:ss");
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("Hbasereader 常量列不支持您配置的列类型"));
        }

    }
}