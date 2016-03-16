package com.alibaba.datax.plugin.reader.hbase11xreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import junit.framework.Assert;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * test
 * Created by shf on 16/3/22.
 */
public class Hbase11xHelperTest {

    @Test
    public void testGetHbaseConnection() {
        //hbaseConfig为blank
        org.apache.hadoop.hbase.client.Connection hConnection;
        String hbaseConfig = " ";
        try {
            hConnection= Hbase11xHelper.getHbaseConnection(hbaseConfig);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("读 Hbase 时需要配置hbaseConfig，其内容为 Hbase 连接信息，请联系 Hbase PE 获取该信息."));
        }

        //hbaseConfig为空
        hbaseConfig = "{}";
        try {
            hConnection= Hbase11xHelper.getHbaseConnection(hbaseConfig);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("hbaseConfig不能为空Map结构!"));
        }
        //hbaseConfig非法
        hbaseConfig = "{@@@@@@@$$$$$}";
        try {
            hConnection= Hbase11xHelper.getHbaseConnection(hbaseConfig);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("获取Hbase连接时出错"));
        }

        //hbaseConfig正常
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        hConnection= Hbase11xHelper.getHbaseConnection(hbaseConfig);
        assertNotNull(hConnection);
    }

    @Test
    public void testGetTable() {
        //table正常
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","users");

        org.apache.hadoop.hbase.client.Table hTable = null;
        hTable = Hbase11xHelper.getTable(configuration);
        assertNotNull(hTable);

        //table 不存在
        configuration.set("table","usersXXXXXXXXXXXXX");
        try {
            hTable = Hbase11xHelper.getTable(configuration);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("不存在, 请检查您的配置 或者 联系 Hbase 管理员"));
        }
    }

    @Test
    public void testGetRegionLocator()  {
        //正常获取
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","users");

        RegionLocator regionLocator= Hbase11xHelper.getRegionLocator(configuration);
        System.out.println(regionLocator.toString());
        assertNotNull(regionLocator);
    }

    @Test
    public void testCloseConnection() throws Exception {

    }

    @Test
    public void testCloseAdmin() throws Exception {

    }

    @Test
    public void testCloseTable() throws Exception {

    }

    @Test
    public void testCloseResultScanner() throws Exception {

    }

    @Test
    public void testCloseRegionLocator() throws Exception {

    }

    @Test
    public void testCheckHbaseTable()throws Exception {
        //正常table
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","users");

        org.apache.hadoop.hbase.client.Connection hConnection= Hbase11xHelper.getHbaseConnection(hbaseConfig);
        Admin admin = hConnection.getAdmin();
        TableName hTableName = TableName.valueOf("users");
        try {
            Hbase11xHelper.checkHbaseTable(admin,hTableName);
        } catch (Exception e) {
            fail(" have DataXException");
        }
        Hbase11xHelper.checkHbaseTable(admin,hTableName);

        //不存table
        TableName hTableName2 = TableName.valueOf("users111111111");
        try {
            Hbase11xHelper.checkHbaseTable(admin,hTableName2);
            fail("DataXException is not thrown as expected");
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("不存在, 请检查您的配置 或者 联系 Hbase 管理员"));
        }

        //disable table
        hTableName2= TableName.valueOf("disable");
        try {
            Hbase11xHelper.checkHbaseTable(admin,hTableName2);
            fail("DataXException is not thrown as expected");
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("is disabled, 请检查您的配置 或者 联系 Hbase 管理员."));
        }
    }

    @Test
    public void testConvertUserStartRowkey()  {
        //正常
        String startkey = "xiaoming";
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        configuration.set(Key.START_ROWKEY,startkey);
        configuration.set(Key.IS_BINARY_ROWKEY,false);
        Assert.assertTrue(
                compare2byte(Bytes.toBytes(startkey),
                        Hbase11xHelper.convertUserStartRowkey(configuration)
                )
        );
        configuration.set(Key.IS_BINARY_ROWKEY,true);
        Assert.assertTrue(
                compare2byte(Bytes.toBytesBinary(startkey),
                        Hbase11xHelper.convertUserStartRowkey(configuration)
                )
        );

        //blank
        String startkey2 = "";
        configuration.set(Key.START_ROWKEY,startkey2);
        Assert.assertTrue(
                compare2byte(HConstants.EMPTY_BYTE_ARRAY,
                        Hbase11xHelper.convertUserStartRowkey(configuration)
                )
        );
    }

    @Test
    public void testConvertUserEndRowkey()  {
        //正常
        String endkey = "lisi_";
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        configuration.set(Key.END_ROWKEY,endkey);
        configuration.set(Key.IS_BINARY_ROWKEY,false);
        Assert.assertTrue(
                compare2byte(Bytes.toBytes(endkey),
                        Hbase11xHelper.convertUserEndRowkey(configuration)
                )
        );
        configuration.set(Key.IS_BINARY_ROWKEY,true);
        System.out.println(Bytes.toStringBinary(Bytes.toBytes("xiaoming_")));
        System.out.println(Bytes.toStringBinary(Bytes.toBytes("lisi_")));

        Assert.assertTrue(
                compare2byte(Bytes.toBytesBinary(endkey),
                        Hbase11xHelper.convertUserEndRowkey(configuration)
                )
        );
        //blank
        String endkey2 = "";
        configuration.set(Key.END_ROWKEY,endkey2);
        Assert.assertTrue(
                compare2byte(HConstants.EMPTY_BYTE_ARRAY,
                        Hbase11xHelper.convertUserEndRowkey(configuration)
                )
        );
    }

    @Test
    public void testConvertInnerStartRowkey()  {
        //正常
        String startkey = "xiaoming";
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        configuration.set(Key.START_ROWKEY,startkey);
        Assert.assertTrue(
                compare2byte(Bytes.toBytes(startkey),
                        Hbase11xHelper.convertInnerStartRowkey(configuration)
                )
        );
        //空
        String startkey2 = "";
        configuration.set(Key.START_ROWKEY,startkey2);
        Assert.assertTrue(
                compare2byte(HConstants.EMPTY_BYTE_ARRAY,
                        Hbase11xHelper.convertInnerStartRowkey(configuration)
                )
        );

    }

    @Test
    public void testConvertInnerEndRowkey()  {
        //正常
        String endkey = "lisi";
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        configuration.set(Key.END_ROWKEY,endkey);
        Assert.assertTrue(
                compare2byte(Bytes.toBytes(endkey),
                        Hbase11xHelper.convertInnerEndRowkey(configuration)
                )
        );
        //blank
        String endkey2 = "";
        configuration.set(Key.END_ROWKEY,endkey2);
        Assert.assertTrue(
                compare2byte(HConstants.EMPTY_BYTE_ARRAY,
                        Hbase11xHelper.convertInnerEndRowkey(configuration)
                )
        );
    }

    @Test
    public void testIsRowkeyColumn()  {
        //rowkey
        Assert.assertTrue(Hbase11xHelper.isRowkeyColumn("rowkey"));
        //非rowkey
        Assert.assertFalse(Hbase11xHelper.isRowkeyColumn("CF1:Q1"));
    }

    @Test
    public void testParseColumnOfNormalMode() {
        //正常类型/常量
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"name\":\"cf1:q2\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        List columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        List<Map> columns = configuration.getList(Key.COLUMN, Map.class);
        List<HbaseColumnCell> hbaseColumnCells1 = Hbase11xHelper.parseColumnOfNormalMode(columns);
        List<HbaseColumnCell> hbaseColumnCells2 = new ArrayList<HbaseColumnCell>();

        hbaseColumnCells2.add(new HbaseColumnCell.Builder(ColumnType.STRING).columnName("rowkey").build());
        hbaseColumnCells2.add(new HbaseColumnCell.Builder(ColumnType.INT).columnName("cf1:q1").build());
        hbaseColumnCells2.add(new HbaseColumnCell.Builder(ColumnType.DATE).columnName("cf1:q2").dateformat("yy-MM-dd hh:MM:ss").build());
        hbaseColumnCells2.add(new HbaseColumnCell.Builder(ColumnType.STRING).columnValue("qiran").build());

        Assert.assertTrue(hbaseColumnCells1.size() == hbaseColumnCells2.size());
        for(int i =0;i<hbaseColumnCells1.size();i++){
            Assert.assertTrue(compare2HbaseColumnCell(hbaseColumnCells1.get(i),hbaseColumnCells2.get(i)));
        }

        //type非法类型
        column = "[{\"name\":\"rowkey\",\"type\":\"stringstring\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        columns = configuration.getList(Key.COLUMN, Map.class);
        try {
            hbaseColumnCells1 = Hbase11xHelper.parseColumnOfNormalMode(columns);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("Hbasereader 不支持该类型"));
        }

        //type为空
        column = "[{\"name\":\"rowkey\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        columns = configuration.getList(Key.COLUMN, Map.class);
        try {
            hbaseColumnCells1 = Hbase11xHelper.parseColumnOfNormalMode(columns);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("Hbasereader 不支持该类型"));
        }

        //date format为空
        column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"name\":\"cf1:q2\",\"type\":\"date\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        columns = configuration.getList(Key.COLUMN, Map.class);
        hbaseColumnCells1 = Hbase11xHelper.parseColumnOfNormalMode(columns);
        hbaseColumnCells2 = new ArrayList<HbaseColumnCell>();

        hbaseColumnCells2.add(new HbaseColumnCell.Builder(ColumnType.STRING).columnName("rowkey").build());
        hbaseColumnCells2.add(new HbaseColumnCell.Builder(ColumnType.INT).columnName("cf1:q1").build());
        hbaseColumnCells2.add(new HbaseColumnCell.Builder(ColumnType.DATE).columnName("cf1:q2").dateformat("yyyy-MM-dd HH:mm:ss").build());
        hbaseColumnCells2.add(new HbaseColumnCell.Builder(ColumnType.STRING).columnValue("qiran").build());

        Assert.assertTrue(hbaseColumnCells1.size() == hbaseColumnCells2.size());
        for(int i =0;i<hbaseColumnCells1.size();i++){
            Assert.assertTrue(compare2HbaseColumnCell(hbaseColumnCells1.get(i),hbaseColumnCells2.get(i)));
        }

        //date type name value全空
        column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"type\":\"date\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        columns = configuration.getList(Key.COLUMN, Map.class);
        try {
            hbaseColumnCells1 = Hbase11xHelper.parseColumnOfNormalMode(columns);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Hbasereader 在 normal 方式读取时则要么是 type + name + format 的组合"));
        }

        //正常type name&value为空
        column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"type\":\"string\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        columns = configuration.getList(Key.COLUMN, Map.class);
        try {
            hbaseColumnCells1 = Hbase11xHelper.parseColumnOfNormalMode(columns);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Hbasereader 在 normal 方式读取时，其列配置中，如果类型不是时间，则要么是 type + name 的组合"));
        }

    }

    @Test
    public void testParseColumnOfMultiversionMode() {
        //正常
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"name\":\"cf1:q2\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}]";
        List columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        List<Map> columns = configuration.getList(Key.COLUMN, Map.class);

        HashMap<String,HashMap<String,String>> multiColumn1  = Hbase11xHelper.parseColumnOfMultiversionMode(columns);

        HashMap<String,HashMap<String,String>> multiColumn2 =new HashMap<String,HashMap<String,String>>();

        HashMap<String, String> rowkeyMap = new  HashMap<String, String>();
        HashMap<String, String> cf1Map = new  HashMap<String, String>();
        HashMap<String, String> cf2Map = new  HashMap<String, String>();
        rowkeyMap.put("type","string");
        cf1Map.put("type","int");
        cf2Map.put("type","date");
        cf2Map.put("format","yy-MM-dd hh:MM:ss");

        multiColumn2.put("rowkey",rowkeyMap);
        multiColumn2.put("cf1:q1",cf1Map);
        multiColumn2.put("cf2:q2",cf2Map);

        //这样判断两个hashmap相等有问题
        System.out.println(JSON.toJSONString(multiColumn1));
        System.out.println(JSON.toJSONString(multiColumn2));
        assertEquals(JSON.toJSONString(multiColumn1),JSON.toJSONString(multiColumn1));

        //name 不是 列族:列名格式
        column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1q1\",\"type\":\"int\"}," +
                "{\"name\":\"cf2q2\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        columns = configuration.getList(Key.COLUMN, Map.class);

        try {
            multiColumn1  = Hbase11xHelper.parseColumnOfMultiversionMode(columns);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("Hbasereader 中，column 的列配置格式应该是：列族:列名"));
        }

    }

    @Test
    public void testSplit() throws Exception {
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","hbasereader_big_all_type");
        String column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"info:age\",\"type\":\"string\"}," +
                "{\"name\":\"info:birthday\",\"type\":\"date\",\"format\":\"yy-MM-dd\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        List columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.set(Key.MODE,"normal");


        //不设置 start end
        configuration.set(Constant.RANGE + "." + Key.START_ROWKEY,"");
        configuration.set(Constant.RANGE + "." + Key.END_ROWKEY,"");
        //注意 必须先validateParameter,它重设置了startkey/endkey
        Hbase11xHelper.validateParameter(configuration);
        List<Configuration> list =  Hbase11xHelper.split(configuration);
        System.out.println(JSON.toJSONString(list));
        assertEquals(list.size(),2);

        //设置start在region1中间,end在region2
        configuration.set(Constant.RANGE + "." + Key.START_ROWKEY,"lisi_54822_");
        configuration.set(Constant.RANGE + "." + Key.END_ROWKEY,"lisi_54826_");
        list.clear();
        Hbase11xHelper.validateParameter(configuration);
        list =  Hbase11xHelper.split(configuration);
        System.out.println(JSON.toJSONString(list));
        assertEquals(list.size(),2);

        //设置start,end在region2
        configuration.set(Constant.RANGE + "." + Key.START_ROWKEY,"lisi_54825_");
        configuration.set(Constant.RANGE + "." + Key.END_ROWKEY,"lisi_54826_");
        list.clear();
        Hbase11xHelper.validateParameter(configuration);
        list =  Hbase11xHelper.split(configuration);
        System.out.println(JSON.toJSONString(list));
        assertEquals(list.size(),1);
        //设置start,end在region1
        configuration.set(Constant.RANGE + "." + Key.START_ROWKEY,"lisi_54822_");
        configuration.set(Constant.RANGE + "." + Key.END_ROWKEY,"lisi_54823_");
        list.clear();
        Hbase11xHelper.validateParameter(configuration);
        list =  Hbase11xHelper.split(configuration);
        System.out.println(JSON.toJSONString(list));
        assertEquals(list.size(),1);

    }

    @Test
    public void testValidateParameter() throws Exception {
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();

        //HBASE_CONFIG 空
        configuration.set(Key.HBASE_CONFIG,"");
        try {
            Hbase11xHelper.validateParameter(configuration);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("[hbaseConfig]是必填参数，不允许为空或者留白"));
        }
        //table 空
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set(Key.HBASE_CONFIG,hbaseConfig);
        configuration.set(Key.TABLE,"");
        try {
            Hbase11xHelper.validateParameter(configuration);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("[table]是必填参数，不允许为空或者留白"));
        }


        //validate mode  mode 为null
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set(Key.HBASE_CONFIG,hbaseConfig);
        configuration.set(Key.TABLE,"users");
        configuration.set(Key.MODE,"");
        try {
            Hbase11xHelper.validateParameter(configuration);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("[mode]是必填参数，不允许为空或者留白"));
        }

        //validate mode  column 为null
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set(Key.HBASE_CONFIG,hbaseConfig);
        configuration.set(Key.TABLE,"users");
        configuration.set(Key.MODE,"normal");
        configuration.set(Key.COLUMN,null);
        try {
            Hbase11xHelper.validateParameter(configuration);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("您配置的column为空,Hbase必须配置 column"));
        }

        //validate mode  column 为empty
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set(Key.HBASE_CONFIG,hbaseConfig);
        configuration.set(Key.TABLE,"users");

        String column = "[]";
        List columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        try {
           Hbase11xHelper.validateParameter(configuration);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("您配置的column为空,Hbase必须配置 column"));
        }
        //validate mode  normal maxversion
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set(Key.HBASE_CONFIG,hbaseConfig);
        configuration.set(Key.TABLE,"users");
        configuration.set(Key.MODE,"normal");
        column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"name\":\"cf1:q2\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.set(Key.MAX_VERSION,"1");
        try {
            Hbase11xHelper.validateParameter(configuration);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("不能配置无关项：maxVersion"));
        }

        //validate mode  MultiVersionFixedColumn maxversion null
        configuration.set(Key.MODE,"MultiVersionFixedColumn");
        configuration.set(Key.MAX_VERSION,null);
        try {
            Hbase11xHelper.validateParameter(configuration);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("必须配置：maxVersion"));
        }
        //validate mode  MultiVersionFixedColumn maxversion<0 不等于-1
        configuration.set(Key.MODE,"MultiVersionFixedColumn");
        configuration.set(Key.MAX_VERSION,"-2");
        try {
            Hbase11xHelper.validateParameter(configuration);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("配置的 maxVersion 值错误"));
        }
        //validate mode  MultiVersionFixedColumn maxversion=1
        configuration.set(Key.MODE,"MultiVersionFixedColumn");
        configuration.set(Key.MAX_VERSION,"1");
        try {
            Hbase11xHelper.validateParameter(configuration);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("配置的 maxVersion 值错误"));
        }
        //validate mode  非法
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set(Key.HBASE_CONFIG,hbaseConfig);
        configuration.set(Key.TABLE,"users");
        configuration.set(Key.MODE,"modexxxx");
        column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"name\":\"cf1:q2\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        try {
            Hbase11xHelper.validateParameter(configuration);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("不支持该 mode 类型"));
        }

        //非法encoding
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set(Key.HBASE_CONFIG,hbaseConfig);
        configuration.set(Key.TABLE,"users");
        configuration.set(Key.MODE,"normal");
        column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"name\":\"cf1:q2\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.remove(Key.MAX_VERSION);
        configuration.set(Key.ENCODING,"XXX");
        try {
            Hbase11xHelper.validateParameter(configuration);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("不支持您所配置的编码"));
        }

        //startRowkey 不为为null 和 ""
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set(Key.HBASE_CONFIG,hbaseConfig);
        configuration.set(Key.TABLE,"users");
        configuration.set(Key.MODE,"normal");
        column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"name\":\"cf1:q2\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.set(Key.ENCODING,null);
        configuration.set(Constant.RANGE + "." + Key.START_ROWKEY,"aaa");
        Hbase11xHelper.validateParameter(configuration);
        assertTrue(configuration.getString(Key.START_ROWKEY).equals("aaa"));

        //startRowkey为""
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set(Key.HBASE_CONFIG,hbaseConfig);
        configuration.set(Key.TABLE,"users");
        configuration.set(Key.MODE,"normal");
        column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"name\":\"cf1:q2\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.remove(Key.START_ROWKEY);
        configuration.set(Constant.RANGE + "." + Key.START_ROWKEY,"");
        Hbase11xHelper.validateParameter(configuration);
        assertNull(configuration.getString(Key.START_ROWKEY));
        //startRowkey为null
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set(Key.HBASE_CONFIG,hbaseConfig);
        configuration.set(Key.TABLE,"users");
        configuration.set(Key.MODE,"normal");
        column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"name\":\"cf1:q2\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.remove(Key.START_ROWKEY);
        configuration.set(Constant.RANGE + "." + Key.START_ROWKEY,null);
        Hbase11xHelper.validateParameter(configuration);
        assertNull(configuration.getString(Key.START_ROWKEY));

        //endRowkey 为null
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set(Key.HBASE_CONFIG,hbaseConfig);
        configuration.set(Key.TABLE,"users");
        configuration.set(Key.MODE,"normal");
        column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"name\":\"cf1:q2\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.remove(Key.END_ROWKEY);
        configuration.set(Constant.RANGE + "." + Key.END_ROWKEY,null);
        Hbase11xHelper.validateParameter(configuration);
        assertNull(configuration.getString(Key.END_ROWKEY));
        //endRowkey为""
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set(Key.HBASE_CONFIG,hbaseConfig);
        configuration.set(Key.TABLE,"users");
        configuration.set(Key.MODE,"normal");
        column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"name\":\"cf1:q2\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.remove(Key.END_ROWKEY);
        configuration.set(Constant.RANGE + "." + Key.END_ROWKEY,"");
        Hbase11xHelper.validateParameter(configuration);
        assertNull(configuration.getString(Key.END_ROWKEY));

        //startRowkey正常
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set(Key.HBASE_CONFIG,hbaseConfig);
        configuration.set(Key.TABLE,"users");
        configuration.set(Key.MODE,"normal");
        column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"cf1:q1\",\"type\":\"int\"}," +
                "{\"name\":\"cf1:q2\",\"type\":\"date\",\"format\":\"yy-MM-dd hh:MM:ss\"}," +
                "{\"value\":\"qiran\",\"type\":\"string\"}]";
        columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.set(Constant.RANGE + "." + Key.END_ROWKEY,"bbb");
        Hbase11xHelper.validateParameter(configuration);
        assertEquals(configuration.getString(Key.END_ROWKEY),"bbb");
    }

    public  boolean compare2byte(byte[] data1, byte[] data2) {
        if (data1 == null && data2 == null) {
            return true;
        }
        if (data1 == null || data2 == null) {
            return false;
        }
        if (data1 == data2) {
            return true;
        }
        if(data1.length != data2.length){
            return false;
        }
        boolean bEquals = true;
        int i;
        for (i = 0; i < data1.length && i < data2.length; i++) {
            if (data1[i] != data2[i]) {
                bEquals = false;
                break;
            }
        }
        return bEquals;
    }

    public boolean compare2HbaseColumnCell(HbaseColumnCell data1,HbaseColumnCell data2){
        if (data1 == null && data2 == null) {
            return true;
        }
        if (data1 == null || data2 == null) {
            return false;
        }
        if (data1 == data2) {
            return true;
        }
        boolean bEquals = false;
        if(
                ( (data1.getColumnType().toString() == null && data2.getColumnType().toString() == null)|| (data1.getColumnType().toString() .equals(data2.getColumnType().toString()))) &&
                ( (data1.getColumnName() == null && data2.getColumnName() == null) || data1.getColumnName().equals(data2.getColumnName()) )&&
                ( (data1.getColumnFamily() == null && data2.getColumnFamily() == null) || (true == this.compare2byte(data1.getColumnFamily(),data2.getColumnFamily())) )&&
                ( (data1.getQualifier() == null && data2.getQualifier() == null) || (true == this.compare2byte(data1.getQualifier(),data2.getQualifier())) )&&
                ( (data1.getColumnValue() == null && data2.getColumnValue() ==null)|| (data1.getColumnValue().equals(data2.getColumnValue()))) &&
                ( data1.isConstant() == data2.isConstant()) &&
                ( (data1.getDateformat() == null && data2.getDateformat() == null) || (data1.getDateformat().equals(data2.getDateformat())) )
                ){
            bEquals = true;
        }

        return bEquals;
    }
}