package com.alibaba.datax.plugin.reader.otsreader.unittest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.Constant;
import com.alibaba.datax.plugin.reader.otsreader.Key;
import com.alibaba.datax.plugin.reader.otsreader.common.AssertHelper;
import com.alibaba.datax.plugin.reader.otsreader.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSCriticalException;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSMode;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;


/**
 * 测试覆盖点
 * 
 * 测试目的：主要是测试Json到Conf之间的协议映射是否正确，错误是否符合预期
 * 
 * 测试点：
 * 1.正常的所有配置
 * 2.解析默认参数
 * 3.错误的参数（必选字段不填、字段类型错误、字段值为空）
 */
public class OTSConfMultiVersionUnittest {

    /**
     * 测试目的：测试所有参数配置正确的前提下
     * 测试内容：构造协议中定义的所有参数，判断解析结果是否符合预期
     * @throws OTSCriticalException 
     */
    @Test
    public void testConf() throws OTSCriticalException {
        // 生成配置
        Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
        ConfigurationHelper.setDefaultConfig(param);
        ConfigurationHelper.setRange(param);
        ConfigurationHelper.setColumnConfig(param, ConfigurationHelper.getNormalColumn(), null);
        
        // Load
        OTSConf conf = OTSConf.load(param);
        
        // Common
        
        assertEquals("endpoint", conf.getEndpoint());
        assertEquals("accessid", conf.getAccessId());
        assertEquals("accesskey", conf.getAccessKey());
        assertEquals("instancename", conf.getInstanceName());
        assertEquals("tablename", conf.getTableName());
        
        assertEquals(20, conf.getRetry());
        assertEquals(120, conf.getSleepInMilliSecond());
        assertEquals(2, conf.getIoThreadCount());
        assertEquals(2, conf.getMaxConnectCount());
        assertEquals(20000, conf.getSocketTimeoutInMilliSecond());
        assertEquals(20000, conf.getConnectTimeoutInMilliSecond());
        
        assertEquals(OTSMode.MULTI_VERSION, conf.getMode());
        
        // Range
        OTSRange range = new OTSRange();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        begin.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.fromString("a")));
        begin.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.INF_MIN));
        
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        end.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.fromString("c")));
        end.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.INF_MAX));
        
        List<List<PrimaryKeyColumn>> split = new ArrayList<List<PrimaryKeyColumn>>();
        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.fromString("b")));
            split.add(pk);
        }
        range.setBegin(begin);
        range.setEnd(end);
        range.setSplit(split);
        
        AssertHelper.assertOTSRange(range, conf.getRange());
        
        // Column
        List<OTSColumn> columns = new ArrayList<OTSColumn>();
        columns.add(OTSColumn.fromNormalColumn("pk1"));
        columns.add(OTSColumn.fromNormalColumn("pk2"));
        columns.add(OTSColumn.fromNormalColumn("attr1"));
        AssertHelper.assertOTSColumn(columns, conf.getColumn());
    }
    
    /**
     * 测试目的：测试默认参数是否生效
     * 测试内容：所有默认字段都不显示配置，检查Conf中取到的值是否符合预期
     * @throws OTSCriticalException
     */
    @Test
    public void testConfDefaultValue() throws OTSCriticalException {
        // 生成配置
        Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
        ConfigurationHelper.setColumnConfig(param, ConfigurationHelper.getNormalColumn(), null);
        
        // Load
        OTSConf conf = OTSConf.load(param);
        
        // Common
        
        assertEquals("endpoint", conf.getEndpoint());
        assertEquals("accessid", conf.getAccessId());
        assertEquals("accesskey", conf.getAccessKey());
        assertEquals("instancename", conf.getInstanceName());
        assertEquals("tablename", conf.getTableName());
        
        assertEquals(Constant.VALUE.RETRY, conf.getRetry());
        assertEquals(Constant.VALUE.SLEEP_IN_MILLISECOND, conf.getSleepInMilliSecond());
        assertEquals(Constant.VALUE.IO_THREAD_COUNT, conf.getIoThreadCount());
        assertEquals(Constant.VALUE.MAX_CONNECT_COUNT, conf.getMaxConnectCount());
        assertEquals(Constant.VALUE.SOCKET_TIMEOUTIN_MILLISECOND, conf.getSocketTimeoutInMilliSecond());
        assertEquals(Constant.VALUE.CONNECT_TIMEOUT_IN_MILLISECOND, conf.getConnectTimeoutInMilliSecond());
        
        assertEquals(OTSMode.MULTI_VERSION, conf.getMode());
        
        // Range
        AssertHelper.assertOTSRange(null, conf.getRange());
        
        // Column
        List<OTSColumn> columns = new ArrayList<OTSColumn>();
        columns.add(OTSColumn.fromNormalColumn("pk1"));
        columns.add(OTSColumn.fromNormalColumn("pk2"));
        columns.add(OTSColumn.fromNormalColumn("attr1"));
        AssertHelper.assertOTSColumn(columns, conf.getColumn());
    }
    
    private void testMissingParam(String key, String expectMessage) {
        // 生成配置
        Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
        param.remove(key);
        
        if (expectMessage != null) {
            try {
                OTSConf.load(param);
                assertTrue(false);
            } catch (OTSCriticalException e) {
                assertEquals(expectMessage, e.getMessage());
            }
        } 
    }
    
    private void testEmptyParam(String key, String expectMessage) {
        // 生成配置
        Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
        param.set(key, "");
        
        try {
            OTSConf.load(param);
            assertTrue(false);
        } catch (OTSCriticalException e) {
            assertEquals(expectMessage, e.getMessage());
        }
    }
    
    /**
     * 测试目的：测试缺少必选参数是，插件的行为是否符合预期
     * 测试内容：分别将必选参数置为空，期望插件抛出异常，错误消息符合预期
     */
    @Test
    public void testMissingRequiredParameters() {
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
    
    private void testInvalidParam(String key, String value, String expectMessage) throws OTSCriticalException {
        // 生成配置
        Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
        param.set(key, value);
        
        try {
            OTSConf.load(param);
            assertTrue(false);
        } catch (OTSCriticalException e) {
            assertEquals(expectMessage, e.getMessage());
        } catch (DataXException e) {
            assertEquals(expectMessage, e.getMessage());
        }
    }
    
    /**
     * 测试目的：传入非预期的值和类型，插件的行为是否符合预期
     * 测试内容：如：需求为字符串的字段传入非字符串、期望数组但是传入Int
     * @throws OTSCriticalException 
     */
    @Test
    public void testErrorFieldAndValue() throws OTSCriticalException {
        
        testInvalidParam(Constant.KEY.RETRY, "hell", "Code:[Common-00], Describe:[您提供的配置文件存在错误信息，请检查您的作业配置 .] - 任务读取配置文件出错. 配置文件路径[maxRetryTime] 值非法, 期望是整数类型: For input string: \"hell\". 请检查您的配置并作出修改.");
        testInvalidParam(Constant.KEY.SLEEP_IN_MILLISECOND, "hell", "Code:[Common-00], Describe:[您提供的配置文件存在错误信息，请检查您的作业配置 .] - 任务读取配置文件出错. 配置文件路径[retrySleepInMillisecond] 值非法, 期望是整数类型: For input string: \"hell\". 请检查您的配置并作出修改.");
        testInvalidParam(Constant.KEY.MAX_CONNECT_COUNT, "hell", "Code:[Common-00], Describe:[您提供的配置文件存在错误信息，请检查您的作业配置 .] - 任务读取配置文件出错. 配置文件路径[maxConnectCount] 值非法, 期望是整数类型: For input string: \"hell\". 请检查您的配置并作出修改.");
        testInvalidParam(Constant.KEY.IO_THREAD_COUNT, "hell", "Code:[Common-00], Describe:[您提供的配置文件存在错误信息，请检查您的作业配置 .] - 任务读取配置文件出错. 配置文件路径[ioThreadCount] 值非法, 期望是整数类型: For input string: \"hell\". 请检查您的配置并作出修改.");
        testInvalidParam(Constant.KEY.SOCKET_TIMEOUTIN_MILLISECOND, "hell", "Code:[Common-00], Describe:[您提供的配置文件存在错误信息，请检查您的作业配置 .] - 任务读取配置文件出错. 配置文件路径[socketTimeoutInMillisecond] 值非法, 期望是整数类型: For input string: \"hell\". 请检查您的配置并作出修改.");
        testInvalidParam(Constant.KEY.CONNECT_TIMEOUT_IN_MILLISECOND, "hell", "Code:[Common-00], Describe:[您提供的配置文件存在错误信息，请检查您的作业配置 .] - 任务读取配置文件出错. 配置文件路径[connectTimeoutInMillisecond] 值非法, 期望是整数类型: For input string: \"hell\". 请检查您的配置并作出修改.");
        
        testInvalidParam(Key.RANGE, "", "Parse 'range' fail, java.lang.String cannot be cast to java.util.Map");
        testInvalidParam(Key.COLUMN, "", "Parse 'column' fail, java.lang.String cannot be cast to java.util.List");
    }
    
    private void testErrorRangeItem(String key, Object value, String expectMessage) {
        {
            Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
            Map<String, Object> range = new LinkedHashMap<String, Object>();
            
            List<Map<String, Object>> pks = new ArrayList<Map<String, Object>>();
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("type", key);
                column.put("value", value);
                pks.add(column);
            }
            range.put(Constant.KEY.Range.BEGIN, pks);
            param.set(Key.RANGE, range);
            
            try {
                OTSConf.load(param);
                assertTrue(false);
            } catch (OTSCriticalException e) {
                assertEquals(expectMessage, e.getMessage());
            } 
        }
        {
            Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
            Map<String, Object> range = new LinkedHashMap<String, Object>();
            
            List<Map<String, Object>> pks = new ArrayList<Map<String, Object>>();
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("type", key);
                column.put("value", value);
                pks.add(column);
            }
            range.put(Constant.KEY.Range.END, pks);
            param.set(Key.RANGE, range);
            
            try {
                OTSConf.load(param);
                assertTrue(false);
            } catch (OTSCriticalException e) {
                assertEquals(expectMessage, e.getMessage());
            } 
        }
    }
    
    private void testFormatError(String key) {
        {
            Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
            Map<String, Object> range = new LinkedHashMap<String, Object>();
            List<Map<String, Object>> pks = new ArrayList<Map<String, Object>>();
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("value", "INF_MIN");
                pks.add(column);
            }
            range.put(key, pks);
            param.set(Key.RANGE, range);
            try {
                OTSConf.load(param);
                assertTrue(false);
            } catch (OTSCriticalException e) {
                assertEquals("Parse 'range' fail, the column must include 'type' and 'value'.", e.getMessage());
            } 
        }
        {
            Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
            Map<String, Object> range = new LinkedHashMap<String, Object>();
            List<List<Object>> pks = new ArrayList<List<Object>>();
            {
                List<Object> column = new ArrayList<Object>();
                column.add("INF_MIN");
                pks.add(column);
            }
            range.put(key, pks);
            param.set(Key.RANGE, range);
            try {
                OTSConf.load(param);
                assertTrue(false);
            } catch (OTSCriticalException e) {
                assertEquals("Parse 'range' fail, input primary key column must be map object, but input type:class java.util.ArrayList", e.getMessage());
            } 
        }
        {
            Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
            Map<String, Object> range = new LinkedHashMap<String, Object>();
            List<Map<String, Object>> pks = new ArrayList<Map<String, Object>>();
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                pks.add(column);
            }
            range.put(key, pks);
            param.set(Key.RANGE, range);
            try {
                OTSConf.load(param);
                assertTrue(false);
            } catch (OTSCriticalException e) {
                assertEquals("Parse 'range' fail, the column must include 'type' and 'value'.", e.getMessage());
            } 
        }
    }
    
    @Test
    public void testRange() throws OTSCriticalException {
        
        // 1.类型, 输入INF_MIN、INF_MAX、string、int、binary解析正确，输入double、bool、xx错误
        {
            Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
            ConfigurationHelper.setColumnConfig(param, ConfigurationHelper.getNormalColumn(), null);
            Map<String, Object> range = new LinkedHashMap<String, Object>();
            
            List<Map<String, Object>> pks = new ArrayList<Map<String, Object>>();
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("type", "INF_MIN");
                pks.add(column);
            }
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("type", "INF_MAX");
                pks.add(column);
            }
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("type", "string");
                column.put("value", "a");
                pks.add(column);
            }
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("type", "int");
                column.put("value", "20");
                pks.add(column);
            }
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("type", "binary");
                column.put("value", Base64.encodeBase64String("hell".getBytes()));
                pks.add(column);
            }
            range.put(Constant.KEY.Range.BEGIN, pks);
            range.put(Constant.KEY.Range.END, pks);
            param.set(Key.RANGE, range);
            
            OTSConf conf = OTSConf.load(param);
            OTSRange rangeExpect = new OTSRange();
            List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
            begin.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.INF_MIN));
            begin.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.INF_MAX));
            begin.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.fromString("a")));
            begin.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.fromLong(20)));
            begin.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.fromBinary("hell".getBytes())));
            
            
            List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
            end.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.INF_MIN));
            end.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.INF_MAX));
            end.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.fromString("a")));
            end.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.fromLong(20)));
            end.add(new PrimaryKeyColumn(Constant.VALUE.DEFAULT_NAME, PrimaryKeyValue.fromBinary("hell".getBytes())));
            
            rangeExpect.setBegin(begin);
            rangeExpect.setEnd(end);

            AssertHelper.assertOTSRange(rangeExpect, conf.getRange());
        }
        {
            testErrorRangeItem("double", "1", "Parse 'range' fail, the column type only support :['INF_MIN', 'INF_MAX', 'string', 'int', 'binary']");
            testErrorRangeItem("bool", "false", "Parse 'range' fail, the column type only support :['INF_MIN', 'INF_MAX', 'string', 'int', 'binary']");
            testErrorRangeItem("xx", "yy", "Parse 'range' fail, the column type only support :['INF_MIN', 'INF_MAX', 'string', 'int', 'binary']");
        }
        // 2.format，输入{"value":"INF_MIN/INF_MAX}、{}、[]错误
        {
            testFormatError(Constant.KEY.Range.BEGIN);
            testFormatError(Constant.KEY.Range.END);
            testFormatError(Constant.KEY.Range.SPLIT);
        }
        
        // 3.值的类型,输入字符串正确，非字符串错误
        {
            testErrorRangeItem("int", Integer.valueOf(10), "Parse 'range' fail, the column's 'type' and 'value' must be string value, but type of 'type' is :class java.lang.String, type of 'value' is :class java.lang.Integer");
            testErrorRangeItem("binary", "hell".getBytes(), "Parse 'range' fail, the column's 'type' and 'value' must be string value, but type of 'type' is :class java.lang.String, type of 'value' is :class [B");
        }
    }
    
    private void testErrorColumnItem(String key, Object value, String expectMessage) {
        {
            Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);

            List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("type", key);
                column.put("value", value);
                columns.add(column);
            }
            param.set(Key.COLUMN, columns);
            
            try {
                OTSConf.load(param);
                assertTrue(false);
            } catch (OTSCriticalException e) {
                assertEquals(expectMessage, e.getMessage());
            } 
        }
    }
    
    private void testErrorValueforColumn(List<Map<String, Object>> normalColumn, List<Map<String, Object>> constColumn, String expectMessage) {
        Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.NORMAL);
        
        if (normalColumn != null && constColumn != null) {
            ConfigurationHelper.setColumnConfig(param, normalColumn, constColumn);
        } else if (normalColumn != null) {
            ConfigurationHelper.setColumnConfig(param, normalColumn, null);
        } else if (constColumn != null) {
            ConfigurationHelper.setColumnConfig(param, null, constColumn);
        }
        
        if (expectMessage != null) {
            try {
                OTSConf.load(param);
                assertTrue(false);
            } catch (OTSCriticalException e) {
                assertEquals(expectMessage, e.getMessage());
            }
        }
    }
    
    @Test
    public void testColumn() {
        // Item of Column
        // 1.空数组
        testMissingParam(Key.COLUMN, null);
        
        // 2.类型，float、xx期望错误
        testErrorColumnItem("float", "0.01", "Parse 'column' fail. the const column type only support :['string', 'int', 'double', 'boolean', 'binary']");
        testErrorColumnItem("xx", "yy", "Parse 'column' fail. the const column type only support :['string', 'int', 'double', 'boolean', 'binary']");
        // 3.format，{"value":"hell"},{"type":"string"},{},[],
        {
            {
                Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
                List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
                {
                    Map<String, Object> column = new LinkedHashMap<String, Object>();
                    column.put("value", "hell");
                    columns.add(column);
                }
                param.set(Key.COLUMN, columns);
                try {
                    OTSConf.load(param);
                    assertTrue(false);
                } catch (OTSCriticalException e) {
                    assertEquals("Parse 'column' fail. the item of column format support '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'.", e.getMessage());
                } 
            }
            {
                Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
                List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
                {
                    Map<String, Object> column = new LinkedHashMap<String, Object>();
                    column.put("type", "string");
                    columns.add(column);
                }
                param.set(Key.COLUMN, columns);
                try {
                    OTSConf.load(param);
                    assertTrue(false);
                } catch (OTSCriticalException e) {
                    assertEquals("Parse 'column' fail. the item of column format support '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'.", e.getMessage());
                } 
            }
            {
                Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
                List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
                {
                    Map<String, Object> column = new LinkedHashMap<String, Object>();
                    columns.add(column);
                }
                param.set(Key.COLUMN, columns);
                try {
                    OTSConf.load(param);
                    assertTrue(false);
                } catch (OTSCriticalException e) {
                    assertEquals("Parse 'column' fail. the item of column format support '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'.", e.getMessage());
                } 
            }
            {
                Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.MULTI_VERSION);
                List<List<Object>> columns = new ArrayList<List<Object>>();
                {
                    List<Object> column = new ArrayList<Object>();
                    columns.add(column);
                }
                param.set(Key.COLUMN, columns);
                try {
                    OTSConf.load(param);
                    assertTrue(false);
                } catch (OTSCriticalException e) {
                    assertEquals("Parse 'column' fail. the item of column must be map object, but input: class java.util.ArrayList", e.getMessage());
                } 
            }
        }
        // 4.值的类型,普通列的值不是string、常量列的值不是字符串
        {
            {
                List<Map<String, Object>> normalColumn = ConfigurationHelper.getNormalColumn();
                {
                    Map<String, Object> column = new LinkedHashMap<String, Object>();
                    column.put("name", 100);
                    normalColumn.add(column);
                }
                testErrorValueforColumn(normalColumn, null, "Parse 'column' fail. the 'name' must be string, but input:class java.lang.Integer");
            }
        }
        // 5.重复，重复普通列
        {
            {
                List<Map<String, Object>> normalColumn = ConfigurationHelper.getNormalColumn();
                testErrorValueforColumn(normalColumn, null, null);
            }
            {
                List<Map<String, Object>> constColumn = ConfigurationHelper.getNormalColumn();
                testErrorValueforColumn(null, constColumn, null);
            }
        }
    }
}
