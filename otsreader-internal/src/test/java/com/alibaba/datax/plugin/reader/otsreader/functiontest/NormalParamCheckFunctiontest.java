package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.Constant;
import com.alibaba.datax.plugin.reader.otsreader.Key;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderMasterProxy;
import com.alibaba.datax.plugin.reader.otsreader.common.AssertHelper;
import com.alibaba.datax.plugin.reader.otsreader.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsreader.common.OtsHelper;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSCriticalException;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSMode;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.internal.model.TableMeta;

public class NormalParamCheckFunctiontest {
    
    private static String tableName = "NormalParamCheckFunctiontest";
    private static Configuration p = ConfigurationHelper.loadConf();
    
    private static OTS ots = null;
    
    @BeforeClass
    public static void setBeforeClass() throws Exception {
        ots = OtsHelper.getOTSInstance();
        
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("Uid", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("Pid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("Mid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("UID", PrimaryKeyType.BINARY);
        
        OtsHelper.createTableSafe(ots, tableMeta);
    }
    
    @AfterClass
    public static void setAfterClass() {
        ots.shutdown();
    }
    
    @Before
    public void setup() {}
    
    @After
    public void teardown() {}
    
    private Map<String, String> getLines() {
        Map<String, String> lines = new LinkedHashMap<String, String>();
        lines.put("endpoint",     "'endpoint':'        "+ p.getString("endpoint") +" '");
        lines.put("accessId",     "'accessId':'         "+ p.getString("accessid") +" '");
        lines.put("accessKey",    "'accessKey':'         "+ p.getString("accesskey") +" '");
        lines.put("instanceName", "'instanceName':' "+ p.getString("instance-name") +" '");
        lines.put("table",        "'table':' "+ tableName +" '");
        lines.put("range",        "'range':{"
                + "'begin':[{'type':'string', 'value':'c'}, {'type':'INF_MIN'}, {'type':'INF_MAX', 'value':''},{'type':'binary', 'value':'"+ Base64.encodeBase64String("world".getBytes()) +"'}], "
                + "'end':  [{'type':'string', 'value':'g'}, {'type':'int', 'value':'100'}, {'type':'int', 'value':'200'}], "
                + "'split':[{'type':'string', 'value':'d'}, {'type':'string', 'value':'e'},{'type':'string', 'value':'f'}]}");
        lines.put("mode",         "'mode':'normal'");
        lines.put("column",       "'column': ["
                + "{'name':'Uid'},{'name':'Pid'},{'name':'attr'},{'type':'string', 'value':'hello'},"
                + "{'type':'int', 'value':'100'},"
                + "{'type':'bool', 'value':'false'},"
                + "{'type':'double', 'value':'192.21'},{'type':'binary', 'value':'"+ Base64.encodeBase64String("red".getBytes()) +"'} ]");
        
        lines.put("maxRetryTime",               "'maxRetryTime': 1");
        lines.put("retryPauseInMillisecond",    "'retryPauseInMillisecond': 10");
        lines.put("ioThreadCount",              "'ioThreadCount': 1");
        lines.put("maxConnectionCount",            "'maxConnectionCount': 2");
        lines.put("socketTimeoutInMillisecond", "'socketTimeoutInMillisecond': 1000");
        lines.put("connectTimeoutInMillisecond","'connectTimeoutInMillisecond': 1000");
        return lines;
    }
    
    /**
     * 测试目的：测试所有配置项配置正确
     * 测试内容：构造一个合法的配置文件，所有配置项都填好正确的值，期望程序解析正常且值符合预期
     * @throws Exception 
     */
    @Test
    public void testConf() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        String json = ConfigurationHelper.linesToJson(getLines());
        Configuration configuration = Configuration.from(json);
        
        proxy.init(configuration);
        
        OTSConf conf = proxy.getConf();
        
        assertEquals(p.getString("endpoint"), conf.getEndpoint());
        assertEquals(p.getString("accessid"), conf.getAccessId());
        assertEquals(p.getString("accesskey"), conf.getAccessKey());
        assertEquals(p.getString("instance-name"), conf.getInstanceName());
        assertEquals(tableName, conf.getTableName());
        assertEquals(OTSMode.NORMAL, conf.getMode());
        
        // Range
        OTSRange range = new OTSRange();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        begin.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("c")));
        begin.add(new PrimaryKeyColumn("Pid", PrimaryKeyValue.INF_MIN));
        begin.add(new PrimaryKeyColumn("Mid", PrimaryKeyValue.INF_MAX));
        begin.add(new PrimaryKeyColumn("UID", PrimaryKeyValue.fromBinary("world".getBytes())));

        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        end.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("g")));
        end.add(new PrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(100)));
        end.add(new PrimaryKeyColumn("Mid", PrimaryKeyValue.fromLong(200)));
        end.add(new PrimaryKeyColumn("UID", PrimaryKeyValue.INF_MAX));
        
        List<PrimaryKeyColumn> split = new ArrayList<PrimaryKeyColumn>();
        {
            split.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("d")));
        }
        {
            split.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("e")));
        }
        {
            split.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("f")));
        }
        range.setBegin(begin);
        range.setEnd(end);
        range.setSplit(split);
        
        AssertHelper.assertOTSRange(range, conf.getRange());
        
        // Other
        assertEquals(1, conf.getRetry());
        assertEquals(10, conf.getRetryPauseInMillisecond());
        assertEquals(1, conf.getIoThreadCount());
        assertEquals(2, conf.getMaxConnectCount());
        assertEquals(1000, conf.getSocketTimeoutInMillisecond());
        assertEquals(1000, conf.getConnectTimeoutInMillisecond());
    }
    
    /**
     * 测试目的：测试所有配置项配置正确
     * 测试内容：构造一个合法的配置文件，所有配置项都填好正确的值，期望程序解析正常且值符合预期
     * @throws Exception 
     */
    @Test
    public void testDefaultConf() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        Map<String, String> lines = this.getLines();
        lines.remove("range");
        lines.remove("maxRetryTime");
        lines.remove("retryPauseInMillisecond");
        lines.remove("ioThreadCount");
        lines.remove("maxConnectionCount");
        lines.remove("socketTimeoutInMillisecond");
        lines.remove("connectTimeoutInMillisecond");
        String json = ConfigurationHelper.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        
        proxy.init(configuration);
        
        OTSConf conf = proxy.getConf();
        
        assertEquals(p.getString("endpoint"), conf.getEndpoint());
        assertEquals(p.getString("accessid"), conf.getAccessId());
        assertEquals(p.getString("accesskey"), conf.getAccessKey());
        assertEquals(p.getString("instance-name"), conf.getInstanceName());
        assertEquals(tableName, conf.getTableName());
        assertEquals(OTSMode.NORMAL, conf.getMode());
        
        // Range
        OTSRange range = new OTSRange();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        begin.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.INF_MIN));
        begin.add(new PrimaryKeyColumn("Pid", PrimaryKeyValue.INF_MIN));
        begin.add(new PrimaryKeyColumn("Mid", PrimaryKeyValue.INF_MIN));
        begin.add(new PrimaryKeyColumn("UID", PrimaryKeyValue.INF_MIN));

        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        end.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.INF_MAX));
        end.add(new PrimaryKeyColumn("Pid", PrimaryKeyValue.INF_MAX));
        end.add(new PrimaryKeyColumn("Mid", PrimaryKeyValue.INF_MAX));
        end.add(new PrimaryKeyColumn("UID", PrimaryKeyValue.INF_MAX));
        
        List<PrimaryKeyColumn> split = new ArrayList<PrimaryKeyColumn>();
        
        range.setBegin(begin);
        range.setEnd(end);
        range.setSplit(split);
        
        AssertHelper.assertOTSRange(range, conf.getRange());
        
        // Other
        assertEquals(Constant.ConfigDefaultValue.RETRY, conf.getRetry());
        assertEquals(Constant.ConfigDefaultValue.RETRY_PAUSE_IN_MILLISECOND, conf.getRetryPauseInMillisecond());
        assertEquals(Constant.ConfigDefaultValue.IO_THREAD_COUNT, conf.getIoThreadCount());
        assertEquals(Constant.ConfigDefaultValue.MAX_CONNECTION_COUNT, conf.getMaxConnectCount());
        assertEquals(Constant.ConfigDefaultValue.SOCKET_TIMEOUT_IN_MILLISECOND, conf.getSocketTimeoutInMillisecond());
        assertEquals(Constant.ConfigDefaultValue.CONNECT_TIMEOUT_IN_MILLISECOND, conf.getConnectTimeoutInMillisecond());
    }
    
    private void testMissingParam(String key, String expectMessage) throws Exception {
        // 生成配置
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        Map<String, String> lines = this.getLines();
        lines.remove(key);
        String json = ConfigurationHelper.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        
        if (expectMessage != null) {
            try {
                proxy.init(configuration);
                assertTrue(false);
            } catch (OTSCriticalException e) {
                assertEquals(expectMessage, e.getMessage());
            }
        } else {
            proxy.init(configuration);
        }
    }
    
    private void testCustomParam(String key, String value,  String expectMessage) throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        Map<String, String> lines = this.getLines();
        lines.put(key, value);
        String json = ConfigurationHelper.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        
        if (expectMessage != null) {
            try {
                proxy.init(configuration);
                assertTrue(false);
            } catch (OTSCriticalException e) {
                assertEquals(expectMessage, e.getMessage());
            } catch (DataXException e) {
                assertEquals(expectMessage, e.getMessage());
            }
        } else {
            proxy.init(configuration);
        }
    }
    
    private void testEmptyParam(String key, String expectMessage) throws Exception {
        testCustomParam(key, "'"+ key +"':''", expectMessage);
    }
    
    /**
     * 测试目的：测试缺少必选参数是，插件的行为是否符合预期
     * 测试内容：分别将必选参数置为空，期望插件抛出异常，错误消息符合预期
     * @throws Exception 
     */
    @Test
    public void testMissingRequiredParameters() throws Exception {
        testMissingParam(Key.OTS_ENDPOINT, "Parse 'endpoint' fail, missing the key.");
        testMissingParam(Key.OTS_ACCESSID, "Parse 'accessId' fail, missing the key.");
        testMissingParam(Key.OTS_ACCESSKEY, "Parse 'accessKey' fail, missing the key.");
        testMissingParam(Key.OTS_INSTANCE_NAME, "Parse 'instanceName' fail, missing the key.");
        testMissingParam(Key.TABLE_NAME, "Parse 'table' fail, missing the key.");
        testMissingParam(Key.MODE, "Parse 'mode' fail, missing the key.");
        
        testEmptyParam(Key.OTS_ENDPOINT, "Parse 'endpoint' fail, input the key is empty string.");
        testEmptyParam(Key.OTS_ACCESSID, "Parse 'accessId' fail, input the key is empty string.");
        testEmptyParam(Key.OTS_ACCESSKEY, "Parse 'accessKey' fail, input the key is empty string.");
        testEmptyParam(Key.OTS_INSTANCE_NAME, "Parse 'instanceName' fail, input the key is empty string.");
        testEmptyParam(Key.TABLE_NAME, "Parse 'table' fail, input the key is empty string.");
        testEmptyParam(Key.MODE, "Parse 'mode' fail, input the key is empty string.");
    }
    
    /**
     * 测试目的：测试系统对Range在各种解析下输入情况解析是否符合预期
     * 测试内容：
     * 1.检查类型是否正确，输入INF_MIN、INF_MAX、string、int、binary解析正确，输入double、bool、xx错误
     * 2.格式是否正确，输入{"value":"INF_MIN/INF_MAX}、{}、[]错误
     * 3.值的类型,输入字符串正确，非字符串错误
     * 4.超过meta个数的限制
     * 5.指定位置的类型和meta不匹配
     * 6.split和meta不匹配
     * 7.逆序
     * @throws Exception 
     */
    @Test
    public void testRange() throws Exception {
        testMissingParam(Key.RANGE, null);
        
        //1.检查类型是否正确，输入INF_MIN、INF_MAX、string、int、binary解析正确，输入double、bool、xx错误
        testCustomParam(
                Key.RANGE, 
                "'range':{"
                + "'begin':[{'type':'string', 'value':'c'}, {'type':'INF_MIN'}, {'type':'INF_MAX', 'value':''},{'type':'binary', 'value':'"+ Base64.encodeBase64String("world".getBytes()) +"'}], "
                + "'end':  [{'type':'string', 'value':'g'}, {'type':'int', 'value':'100'}, {'type':'int', 'value':'200'}], "
                + "'split':[{'type':'string', 'value':'d'}, {'type':'string', 'value':'e'},{'type':'string', 'value':'f'}]}", 
                null);
        
        testCustomParam(
                Key.RANGE, 
                "'range':{"
                + "'begin':[{'type':'string', 'value':'c'}, {'type':'INF_MIN'}, {'type':'INF_MAX', 'value':''},{'type':'binary', 'value':'"+ Base64.encodeBase64String("world".getBytes()) +"'}], "
                + "'end':  [{'type':'string', 'value':'g'}, {'type':'double', 'value':'100'}, {'type':'int', 'value':'200'}], "
                + "'split':[{'type':'string', 'value':'d'}, {'type':'string', 'value':'e'},{'type':'string', 'value':'f'}]}", 
                "Parse 'range' fail, the column type only support :['INF_MIN', 'INF_MAX', 'string', 'int', 'binary']");
        
        testCustomParam(
                Key.RANGE, 
                "'range':{"
                + "'begin':[{'type':'string', 'value':'c'}, {'type':'INF_MIN'}, {'type':'INF_MAX', 'value':''},{'type':'binary', 'value':'"+ Base64.encodeBase64String("world".getBytes()) +"'}], "
                + "'end':  [{'type':'string', 'value':'g'}, {'type':'bool', 'value':'true'}, {'type':'int', 'value':'200'}], "
                + "'split':[{'type':'string', 'value':'d'}, {'type':'string', 'value':'e'},{'type':'string', 'value':'f'}]}", 
                "Parse 'range' fail, the column type only support :['INF_MIN', 'INF_MAX', 'string', 'int', 'binary']");
        
        testCustomParam(
                Key.RANGE, 
                "'range':{"
                + "'begin':[{'type':'string', 'value':'c'}, {'type':'INF_MIN'}, {'type':'INF_MAX', 'value':''},{'type':'binary', 'value':'"+ Base64.encodeBase64String("world".getBytes()) +"'}], "
                + "'end':  [{'type':'string', 'value':'g'}, {'type':'xx', 'value':'true'}, {'type':'int', 'value':'200'}], "
                + "'split':[{'type':'string', 'value':'d'}, {'type':'string', 'value':'e'},{'type':'string', 'value':'f'}]}", 
                "Parse 'range' fail, the column type only support :['INF_MIN', 'INF_MAX', 'string', 'int', 'binary']");
        
        // 2.格式是否正确，输入{"value":"INF_MIN/INF_MAX}、{}、[]错误
        testCustomParam(
                Key.RANGE, 
                "'range':{"
                + "'begin':[{'type':'string', 'value':'c'}, {'value':'INF_MIN'}, {'type':'INF_MAX', 'value':''},{'type':'binary', 'value':'"+ Base64.encodeBase64String("world".getBytes()) +"'}], "
                + "'end':  [{'type':'string', 'value':'g'}, {'type':'double', 'value':'100'}, {'type':'int', 'value':'200'}], "
                + "'split':[{'type':'string', 'value':'d'}, {'type':'string', 'value':'e'},{'type':'string', 'value':'f'}]}", 
                "Parse 'range' fail, the column must include 'type' and 'value'.");
        
        testCustomParam(
                Key.RANGE, 
                "'range':{"
                + "'begin':[{'type':'string', 'value':'c'}, {}, {'type':'INF_MAX', 'value':''},{'type':'binary', 'value':'"+ Base64.encodeBase64String("world".getBytes()) +"'}], "
                + "'end':  [{'type':'string', 'value':'g'}, {'type':'double', 'value':'100'}, {'type':'int', 'value':'200'}], "
                + "'split':[{'type':'string', 'value':'d'}, {'type':'string', 'value':'e'},{'type':'string', 'value':'f'}]}", 
                "Parse 'range' fail, the column must include 'type' and 'value'.");
        
        testCustomParam(
                Key.RANGE, 
                "'range':{"
                + "'begin':[{'type':'string', 'value':'c'}, [], {'type':'INF_MAX', 'value':''},{'type':'binary', 'value':'"+ Base64.encodeBase64String("world".getBytes()) +"'}], "
                + "'end':  [{'type':'string', 'value':'g'}, {'type':'double', 'value':'100'}, {'type':'int', 'value':'200'}], "
                + "'split':[{'type':'string', 'value':'d'}, {'type':'string', 'value':'e'},{'type':'string', 'value':'f'}]}", 
                "Parse 'range' fail, input primary key column must be map object, but input type:class com.alibaba.fastjson.JSONArray");
        
        // 3.值的类型,输入字符串正确，非字符串错误
        testCustomParam(
                Key.RANGE, 
                "'range':{"
                + "'begin':[{'type':'string', 'value':'c'}, {'type':'INF_MIN'}, {'type':'INF_MAX', 'value':''},{'type':'binary', 'value':'"+ Base64.encodeBase64String("world".getBytes()) +"'}], "
                + "'end':  [{'type':'string', 'value':'g'}, {'type':'double', 'value':100}, {'type':'int', 'value':'200'}], "
                + "'split':[{'type':'string', 'value':'d'}, {'type':'string', 'value':'e'},{'type':'string', 'value':'f'}]}", 
                "Parse 'range' fail, the column's 'type' and 'value' must be string value, but type of 'type' is :class java.lang.String, type of 'value' is :class java.lang.Integer");
        
        // 4.超过meta定义的限制
        testCustomParam(
                Key.RANGE, 
                "'range':{"
                + "'begin':[{'type':'string', 'value':'c'}, {'type':'INF_MIN'}, {'type':'INF_MAX', 'value':''},{'type':'binary', 'value':'"+ Base64.encodeBase64String("world".getBytes()) +"'}, {'type':'string', 'value':'c'}], "
                + "'end':  [{'type':'string', 'value':'g'}, {'type':'int', 'value':'100'}, {'type':'int', 'value':'200'}], "
                + "'split':[{'type':'string', 'value':'d'}, {'type':'string', 'value':'e'},{'type':'string', 'value':'f'}]}", 
                "Parse 'range' fail, The 'begin', input primary key column size more than table meta, input size: 5, meta pk size:4");
        
        // 5.对应位置类型不匹配
        testCustomParam(
                Key.RANGE, 
                "'range':{"
                + "'begin':[{'type':'string', 'value':'c'}, {'type':'INF_MIN'}, {'type':'INF_MAX', 'value':''},{'type':'binary', 'value':'"+ Base64.encodeBase64String("world".getBytes()) +"'}], "
                + "'end':  [{'type':'string', 'value':'g'}, {'type':'string', 'value':'100'}, {'type':'int', 'value':'200'}], "
                + "'split':[{'type':'string', 'value':'d'}, {'type':'string', 'value':'e'},{'type':'string', 'value':'f'}]}", 
                "Parse 'range' fail, The 'end', input primary key column type mismath table meta, input type:STRING, meta pk type:INTEGER, index:1");
        
        // 6.split和meta不匹配
        testCustomParam(
                Key.RANGE, 
                "'range':{"
                + "'begin':[{'type':'string', 'value':'c'}], "
                + "'end':  [{'type':'string', 'value':'g'}], "
                + "'split':[{'type':'int', 'value':'100'}]}", 
                "Parse 'range' fail, The 'split', input primary key column type is mismatch partition key, input type: INTEGER, partition key type:STRING, index:0");
        
        // 7.逆序
        testCustomParam(
                Key.RANGE, 
                "'range':{"
                + "'begin':[{'type':'string', 'value':'g'}, {'type':'int', 'value':'100'}, {'type':'int', 'value':'200'}], "
                + "'end':  [{'type':'string', 'value':'c'}, {'type':'INF_MIN'}, {'type':'INF_MAX', 'value':''},{'type':'binary', 'value':'"+ Base64.encodeBase64String("world".getBytes()) +"'}], "
                + "'split':[{'type':'string', 'value':'d'}, {'type':'string', 'value':'e'},{'type':'string', 'value':'f'}]}", 
                "Parse 'range' fail, In 'split', the item value is not descending, index: 0");
        
        testCustomParam(
                Key.RANGE, 
                "'range':{"
                + "'begin':[{'type':'string', 'value':'g'}, {'type':'int', 'value':'100'}, {'type':'int', 'value':'200'}], "
                + "'end':  [{'type':'string', 'value':'c'}, {'type':'INF_MIN'}, {'type':'INF_MAX', 'value':''},{'type':'binary', 'value':'"+ Base64.encodeBase64String("world".getBytes()) +"'}], "
                + "'split':[]}", 
                null);
    }
    
    /**
     * 测试目的：测试Column在各种输入情况下是否符合预期
     * 测试内容：
     * 1.空数组，期望错误
     * 2.指定不存在的Column，期望正确
     * 3.指定PK，期望正确
     * 4.指定常量列，期望正确
     * 5.格式未定义的Column，期望错误
     * 6.类型未定义的常量列，期望错误
     * 7.重复列，期望正确
     * @throws Exception 
     */
    @Test
    public void testColumn() throws Exception {
        // 1.空数组，期望正确
        testCustomParam(
                Key.COLUMN,
                "'column': []",
                "Parse 'column' fail, in mode:'normal', the 'column' must specify at least one column_name or const column.");
        
        // 2.指定不存在的Column，期望正确
        testCustomParam(
                Key.COLUMN,
                "'column': [{'name':'yaya'}]",
                null);
        
        // 3.指定PK，期望正确
        testCustomParam(
                Key.COLUMN,
                "'column': [{'name':'Uid'}]",
                null);
        
        // 4.指定常量列，期望正确
        testCustomParam(
                Key.COLUMN,
                "'column': [{'name':'Uid'}, {'type':'string', 'value':'ssssss'}]",
                null);
        
        // 5.格式未定义的Column，期望错误
        testCustomParam(
                Key.COLUMN,
                "'column': [['name','uid']]",
                "Parse 'column' fail. the item of column must be map object, but input: class com.alibaba.fastjson.JSONArray");
        
        // 6.类型未定义的常量列，期望错误
        testCustomParam(
                Key.COLUMN,
                "'column': [{'xx':'string', 'value':'ssssss'}]",
                "Parse 'column' fail. the item of column format support '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'.");
        
        // 7.重复列，期望正确
        testCustomParam(
                Key.COLUMN,
                "'column': [{'name':'attr1'},{'name':'attr1'}]",
                null);
    }
}
