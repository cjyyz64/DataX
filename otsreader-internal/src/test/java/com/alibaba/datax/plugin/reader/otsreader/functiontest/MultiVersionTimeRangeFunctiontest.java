package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsreader.common.OtsHelper;
import com.alibaba.datax.plugin.reader.otsreader.common.TestHelper;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;
import com.aliyun.openservices.ots.internal.model.TableMeta;


public class MultiVersionTimeRangeFunctiontest {
    private static String tableName = "multiVersion";
    private static Configuration p = ConfigurationHelper.loadConf();
    private static OTS ots = null;
    
    @BeforeClass
    public static void setBeforeClass() throws Exception {
        ots = OtsHelper.getOTSInstance();
        
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("Uid", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("Pid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("UID", PrimaryKeyType.BINARY);
        
        OtsHelper.createTableSafe(ots, tableMeta);
        
        // prepare data
        // 构造1000万行数据，每行3个PK，7个普通列
        long start_version = 100;
        OtsHelper.prepareData(
                ots, tableMeta, 
                0, 100, 
                0, 7,
                start_version);
        // 构造1000万行数据，每行3个PK，10个普通列
        start_version = 101;
        OtsHelper.prepareData(
                ots, tableMeta, 
                50, 150, 
                0, 10,
                start_version);
        
        start_version = 102;
        OtsHelper.prepareData(
                ots, tableMeta, 
                50, 150, 
                0, 10,
                start_version);
        
        start_version = 103;
        OtsHelper.prepareData(
                ots, tableMeta, 
                50, 150, 
                0, 10,
                start_version);
        
        start_version = 104;
        OtsHelper.prepareData(
                ots, tableMeta, 
                50, 150, 
                0, 10,
                start_version);
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
                + "'begin':[], "
                + "'end':  [], "
                + "'split':[]}");
        lines.put("mode",         "'mode':'multiVersion'");
        return lines;
    }
    
    /**
     * 测试目的：测试在默认参数的情况下，导出的数据是否符合预期
     * 测试内容：TimeRange不填，期望导出所有版本的数据，且值符合预期
     * @throws Exception 
     */
    @Test
    public void testDefaultTimeRange() throws Exception {
        Map<String, String> lines = getLines();
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
    
    /**
     * 测试目的：测试在设置参数的情况下，导出的数据是否符合预期
     * 测试内容：同时配置begin和end，期望导出的数据时间全部在begin和end之间，且值符合预期
     * @throws Exception 
     */
    @Test
    public void testCustomBeginAndEnd() throws Exception {
        Map<String, String> lines = getLines();
        lines.put("timeRange",         "'timeRange':{"
                + "'begin':100, "
                + "'end':103"
                + "}");
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
    
    /**
     * 测试目的：测试在设置参数的情况下，导出的数据是否符合预期
     * 测试内容：配置begin，期望导出的数据时间全部在begin和begin之后，且值符合预期
     * @throws Exception 
     */
    @Test
    public void testCustomBegin() throws Exception {
        Map<String, String> lines = getLines();
        lines.put("timeRange",         "'timeRange':{"
                + "'begin':100, "
                + "}");
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
    
    /**
     * 测试目的：测试在设置参数的情况下，导出的数据是否符合预期
     * 测试内容：配置end，期望导出的数据时间全部在end之前，且值符合预期
     * @throws Exception 
     */
    @Test
    public void testCustomEnd() throws Exception {
        Map<String, String> lines = getLines();
        lines.put("timeRange",         "'timeRange':{"
                + "'end':106"
                + "}");
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
    
    /**
     * 测试目的：测试在设置参数的情况下，导出的数据是否符合预期
     * 测试内容：同时配置begin、end、maxVersion，期望导出的数据时间全部在begin和end之间，且同Column的数据在maxVersion限定之内，且值符合预期
     * @throws Exception 
     */
    @Test
    public void testCustomBeginAndEndAndMaxVersion() throws Exception {
        Map<String, String> lines = getLines();
        lines.put("timeRange",         "'timeRange':{"
                + "'begin':101, "
                + "'end':104"
                + "}");
        lines.put("maxVersion",         "'maxVersion':1");
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
}
