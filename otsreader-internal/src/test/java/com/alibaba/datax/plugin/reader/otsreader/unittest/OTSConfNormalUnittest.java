package com.alibaba.datax.plugin.reader.otsreader.unittest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

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
public class OTSConfNormalUnittest {

    /**
     * 测试目的：测试所有参数配置正确的前提下
     * 测试内容：构造协议中定义的所有参数，判断解析结果是否符合预期
     * @throws OTSCriticalException 
     */
    @Test
    public void testConf() throws OTSCriticalException {
        // 生成配置
        Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.NORMAL);
        ConfigurationHelper.setDefaultConfig(param);
        ConfigurationHelper.setRange(param);
        ConfigurationHelper.setColumnConfig(param, ConfigurationHelper.getNormalColumn(), ConfigurationHelper.getConstColumn());
        
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
        assertEquals(2, conf.getConcurrencyCount());
        assertEquals(2, conf.getIoThreadCount());
        assertEquals(2, conf.getMaxConnectCount());
        assertEquals(20000, conf.getSocketTimeoutInMilliSecond());
        assertEquals(20000, conf.getConnectTimeoutInMilliSecond());
        
        assertEquals(OTSMode.NORMAL, conf.getMode());
        
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
        columns.add(OTSColumn.fromConstStringColumn("string_value"));
        columns.add(OTSColumn.fromConstIntegerColumn(-10));
        columns.add(OTSColumn.fromConstDoubleColumn(10.001));
        columns.add(OTSColumn.fromConstBoolColumn(true));
        columns.add(OTSColumn.fromConstBytesColumn("hello".getBytes()));
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
        Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.NORMAL);
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
        assertEquals(Constant.VALUE.CONCURRENCY_READ, conf.getConcurrencyCount());
        assertEquals(Constant.VALUE.IO_THREAD_COUNT, conf.getIoThreadCount());
        assertEquals(Constant.VALUE.MAX_CONNECT_COUNT, conf.getMaxConnectCount());
        assertEquals(Constant.VALUE.SOCKET_TIMEOUTIN_MILLISECOND, conf.getSocketTimeoutInMilliSecond());
        assertEquals(Constant.VALUE.CONNECT_TIMEOUT_IN_MILLISECOND, conf.getConnectTimeoutInMilliSecond());
        
        assertEquals(OTSMode.NORMAL, conf.getMode());
        
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
        Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.NORMAL);
        param.remove(key);
        
        try {
            OTSConf.load(param);
            assertTrue(false);
        } catch (OTSCriticalException e) {
            assertEquals(expectMessage, e.getMessage());
        }
    }
    
    private void testEmptyParam(String key, String expectMessage) {
        // 生成配置
        Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.NORMAL);
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
        Configuration param = ConfigurationHelper.getDefaultConfiguration(OTSMode.NORMAL);
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
     * 测试内容：如：需求为字符串的字段传入非字符串、期望数组但是传入Int、Range不是升序等
     * @throws OTSCriticalException 
     */
    @Test
    public void testErrorFieldAndValue() throws OTSCriticalException {
        
        testInvalidParam(Constant.KEY.RETRY, "hell", "Code:[Common-00], Describe:[您提供的配置文件存在错误信息，请检查您的作业配置 .] - 任务读取配置文件出错. 配置文件路径[maxRetryTime] 值非法, 期望是整数类型: For input string: \"hell\". 请检查您的配置并作出修改.");
        testInvalidParam(Constant.KEY.SLEEP_IN_MILLISECOND, "hell", "Code:[Common-00], Describe:[您提供的配置文件存在错误信息，请检查您的作业配置 .] - 任务读取配置文件出错. 配置文件路径[retrySleepInMillisecond] 值非法, 期望是整数类型: For input string: \"hell\". 请检查您的配置并作出修改.");
        testInvalidParam(Constant.KEY.CONCURRENCY_READ, "hell", "Code:[Common-00], Describe:[您提供的配置文件存在错误信息，请检查您的作业配置 .] - 任务读取配置文件出错. 配置文件路径[concurrencyRead] 值非法, 期望是整数类型: For input string: \"hell\". 请检查您的配置并作出修改.");
        testInvalidParam(Constant.KEY.MAX_CONNECT_COUNT, "hell", "Code:[Common-00], Describe:[您提供的配置文件存在错误信息，请检查您的作业配置 .] - 任务读取配置文件出错. 配置文件路径[maxConnectCount] 值非法, 期望是整数类型: For input string: \"hell\". 请检查您的配置并作出修改.");
        testInvalidParam(Constant.KEY.IO_THREAD_COUNT, "hell", "Code:[Common-00], Describe:[您提供的配置文件存在错误信息，请检查您的作业配置 .] - 任务读取配置文件出错. 配置文件路径[ioThreadCount] 值非法, 期望是整数类型: For input string: \"hell\". 请检查您的配置并作出修改.");
        testInvalidParam(Constant.KEY.SOCKET_TIMEOUTIN_MILLISECOND, "hell", "Code:[Common-00], Describe:[您提供的配置文件存在错误信息，请检查您的作业配置 .] - 任务读取配置文件出错. 配置文件路径[socketTimeoutInMillisecond] 值非法, 期望是整数类型: For input string: \"hell\". 请检查您的配置并作出修改.");
        testInvalidParam(Constant.KEY.CONNECT_TIMEOUT_IN_MILLISECOND, "hell", "Code:[Common-00], Describe:[您提供的配置文件存在错误信息，请检查您的作业配置 .] - 任务读取配置文件出错. 配置文件路径[connectTimeoutInMillisecond] 值非法, 期望是整数类型: For input string: \"hell\". 请检查您的配置并作出修改.");
    
        // Range
        testInvalidParam(Key.RANGE, "", "Parse 'range' fail, java.lang.String cannot be cast to java.util.Map");
        
        // Column
        testInvalidParam(Key.COLUMN, "", "Parse 'column' fail, java.lang.String cannot be cast to java.util.List");
    }
}
