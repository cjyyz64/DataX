package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

public class MultiVersionParamCheckFunctiontest {
    
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
        tableMeta.addPrimaryKeyColumn("UID", PrimaryKeyType.STRING);
        
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
        lines.put("endpoint",     "'endpoint':' "+ p.getString("endpoint") +" '");
        lines.put("accessId",     "'accessId':' "+ p.getString("accessid") +" '");
        lines.put("accessKey",    "'accessKey':' "+ p.getString("accesskey") +" '");
        lines.put("instanceName", "'instanceName':' "+ p.getString("instance-name") +" '");
        lines.put("table",        "'table':' "+ tableName +" '");
        lines.put("range",        "'range':{"
                + "'begin':[{'type':'string', 'value':'c'}, {'type':'INF_MIN'}], "
                + "'end':  [{'type':'string', 'value':'g'}, {'type':'int', 'value':'100'}, {'type':'int', 'value':'200'}], "
                + "'split':[{'type':'string', 'value':'d'}, {'type':'string', 'value':'e'},{'type':'string', 'value':'f'}]}");
        lines.put("mode",         "'mode':'multiVersion'");
        lines.put("column",       "'column': []");
        lines.put("timeRange",    "'timeRange': {'begin':2000, 'end':"+ Long.MAX_VALUE +"}");
        lines.put("maxVersion",               "'maxVersion': 1000");
        
        lines.put("maxRetryTime",               "'maxRetryTime': 1");
        lines.put("retrySleepInMillisecond",    "'retrySleepInMillisecond': 10");
        lines.put("ioThreadCount",              "'ioThreadCount': 1");
        lines.put("maxConnectCount",            "'maxConnectCount': 2");
        lines.put("socketTimeoutInMillisecond", "'socketTimeoutInMillisecond': 1000");
        lines.put("connectTimeoutInMillisecond","'connectTimeoutInMillisecond': 1000");
        return lines;
    }
    
    private String linesToJson(Map<String, String> lines) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        Set<Entry<String, String>> entrys = lines.entrySet();
        int i = 0;
        for (Entry<String, String> e : entrys) {
            if (i == (entrys.size() - 1)) {
                sb.append(e.getValue());
            } else {
                sb.append(e.getValue() + ",");
            }
            i++;
        }
        sb.append("}");
        return sb.toString();
    }
    
    /**
     * 测试目的：测试所有配置项配置正确
     * 测试内容：构造一个合法的配置文件，所有配置项都填好正确的值，期望程序解析正常且值符合预期
     * @throws Exception 
     */
    @Test
    public void testConf() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        String json = linesToJson(this.getLines());
        Configuration configuration = Configuration.from(json);
        
        proxy.init(configuration);
        
        OTSConf conf = proxy.getConf();
        
        assertEquals(p.getString("endpoint"), conf.getEndpoint());
        assertEquals(p.getString("accessid"), conf.getAccessId());
        assertEquals(p.getString("accesskey"), conf.getAccessKey());
        assertEquals(p.getString("instance-name"), conf.getInstanceName());
        assertEquals(tableName, conf.getTableName());
        assertEquals(OTSMode.MULTI_VERSION, conf.getMode());
        
        // Range
        OTSRange range = new OTSRange();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        begin.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("c")));
        begin.add(new PrimaryKeyColumn("Pid", PrimaryKeyValue.INF_MIN));
        begin.add(new PrimaryKeyColumn("Mid", PrimaryKeyValue.INF_MIN));
        begin.add(new PrimaryKeyColumn("UID", PrimaryKeyValue.INF_MIN));

        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        end.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("g")));
        end.add(new PrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(100)));
        end.add(new PrimaryKeyColumn("Mid", PrimaryKeyValue.fromLong(200)));
        end.add(new PrimaryKeyColumn("UID", PrimaryKeyValue.INF_MAX));
        
        List<List<PrimaryKeyColumn>> split = new ArrayList<List<PrimaryKeyColumn>>();
        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("d")));
            pk.add(new PrimaryKeyColumn("Pid", PrimaryKeyValue.INF_MIN));
            pk.add(new PrimaryKeyColumn("Mid", PrimaryKeyValue.INF_MIN));
            pk.add(new PrimaryKeyColumn("UID", PrimaryKeyValue.INF_MIN));
            split.add(pk);
        }
        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("e")));
            pk.add(new PrimaryKeyColumn("Pid", PrimaryKeyValue.INF_MIN));
            pk.add(new PrimaryKeyColumn("Mid", PrimaryKeyValue.INF_MIN));
            pk.add(new PrimaryKeyColumn("UID", PrimaryKeyValue.INF_MIN));
            split.add(pk);
        }
        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("f")));
            pk.add(new PrimaryKeyColumn("Pid", PrimaryKeyValue.INF_MIN));
            pk.add(new PrimaryKeyColumn("Mid", PrimaryKeyValue.INF_MIN));
            pk.add(new PrimaryKeyColumn("UID", PrimaryKeyValue.INF_MIN));
            split.add(pk);
        }
        range.setBegin(begin);
        range.setEnd(end);
        range.setSplit(split);
        
        AssertHelper.assertOTSRange(range, conf.getRange());
        
        // TimeRange
        assertEquals(2000, conf.getMulti().getTimeRange().getMinStamp());
        assertEquals(Long.MAX_VALUE, conf.getMulti().getTimeRange().getMaxStamp());
        
        // Version
        assertEquals(1000, conf.getMulti().getMaxVersion());
        
        // Other
        assertEquals(1, conf.getRetry());
        assertEquals(10, conf.getSleepInMilliSecond());
        assertEquals(1, conf.getIoThreadCount());
        assertEquals(2, conf.getMaxConnectCount());
        assertEquals(1000, conf.getSocketTimeoutInMilliSecond());
        assertEquals(1000, conf.getConnectTimeoutInMilliSecond());
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
        lines.remove("timeRange");
        lines.remove("maxVersion");
        lines.remove("column");
        String json = linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        
        proxy.init(configuration);
        
        OTSConf conf = proxy.getConf();
        
        assertEquals(p.getString("endpoint"), conf.getEndpoint());
        assertEquals(p.getString("accessid"), conf.getAccessId());
        assertEquals(p.getString("accesskey"), conf.getAccessKey());
        assertEquals(p.getString("instance-name"), conf.getInstanceName());
        assertEquals(tableName, conf.getTableName());
        assertEquals(OTSMode.MULTI_VERSION, conf.getMode());
        
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
        
        List<List<PrimaryKeyColumn>> split = new ArrayList<List<PrimaryKeyColumn>>();
        
        range.setBegin(begin);
        range.setEnd(end);
        range.setSplit(split);
        
        AssertHelper.assertOTSRange(range, conf.getRange());
        
        // TimeRange
        assertEquals(0, conf.getMulti().getTimeRange().getMinStamp());
        assertEquals(Long.MAX_VALUE, conf.getMulti().getTimeRange().getMaxStamp());
        // Version
        assertEquals(Integer.MAX_VALUE, conf.getMulti().getMaxVersion());
        
        // Other
        assertEquals(Constant.VALUE.RETRY, conf.getRetry());
        assertEquals(Constant.VALUE.SLEEP_IN_MILLISECOND, conf.getSleepInMilliSecond());
        assertEquals(Constant.VALUE.IO_THREAD_COUNT, conf.getIoThreadCount());
        assertEquals(Constant.VALUE.MAX_CONNECT_COUNT, conf.getMaxConnectCount());
        assertEquals(Constant.VALUE.SOCKET_TIMEOUTIN_MILLISECOND, conf.getSocketTimeoutInMilliSecond());
        assertEquals(Constant.VALUE.CONNECT_TIMEOUT_IN_MILLISECOND, conf.getConnectTimeoutInMilliSecond());
    }
    
    private void testMissingParam(String key, String expectMessage) throws Exception {
        // 生成配置
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        Map<String, String> lines = this.getLines();
        lines.remove(key);
        String json = linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        
        if (expectMessage != null) {
            try {
                proxy.init(configuration);
                assertTrue(false);
            } catch (OTSCriticalException e) {
                assertEquals(expectMessage, e.getMessage());
            }
        } 
    }
    
    private void testEmptyParam(String key, String expectMessage) throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        Map<String, String> lines = this.getLines();
        lines.put(key, "'"+ key +"':''");
        String json = linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        
        if (expectMessage != null) {
            try {
                proxy.init(configuration);
                assertTrue(false);
            } catch (OTSCriticalException e) {
                assertEquals(expectMessage, e.getMessage());
            }
        } 
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
}
