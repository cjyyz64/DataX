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

/**
 * 测试在多版本中，各种不同的Range设置，期望导出的数据符合预期
 */
public class NormalRangeStringFunctiontest {
    
    private static String tableName = "normal";
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
        OtsHelper.prepareData(
                ots, tableMeta, 
                0, 100, 
                0, 7);
        // 构造1000万行数据，每行3个PK，10个普通列
        OtsHelper.prepareData(
                ots, tableMeta, 
                50, 150, 
                0, 10);
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
        lines.put("mode",         "'mode':'normal'");
        lines.put("column",       "'column':[{'name':'Uid'},{'name':'Pid'},{'name':'UID'},{'name':'col_000000'},{'name':'col_000000'},{'name':'col_000000'}]");
        return lines;
    }

    /**
     * 测试目的：在默认参数先，系统是否是符合预期
     * 测试内容：配置文件只填写必选参数，测试系统是否能够导出所有数据，且数据符合预期
     * @throws Exception
     */
    @Test
    public void testDefaulRange() throws Exception {
        Map<String, String> lines = getLines();
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
    
    /**
     * 测试目的：在用户指定参数的情况，观察系统是否符合预期
     * 测试内容：begin和end都配置特定的范围，测试系统是否仅仅导入指定范围的数据，且数据符合预期
     * @throws Exception 
     */
    @Test
    public void testCustomRangeBeginAndEnd() throws Exception {
        Map<String, String> lines = getLines();
        lines.put("range", "'range':{"
                + "'begin':[{'type':'string', 'value':'000100'}], "
                + "'end':  [{'type':'string', 'value':'000120'}]}");
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
    
    /**
     * 测试目的：在用户指定参数的情况，观察系统是否符合预期
     * 测试内容：配置了多个split point，期望程序将导入拆分为多个子任务，测试系统是否能够导出所有数据，且数据符合预期
     * @throws Exception 
     */
    @Test
    public void testCustomRangeSplit() throws Exception {
        Map<String, String> lines = getLines();
        lines.put("range", "'range':{"
                + "'split':[{'type':'string', 'value':'000050'},{'type':'string', 'value':'000080'},{'type':'string', 'value':'000100'},{'type':'string', 'value':'000130'}]}");
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
    
    /**
     * 测试目的：在用户指定参数的情况，观察系统是否符合预期
     * 测试内容：同时配置begin、end、split，期望程序将导入拆分为多个子任务，测试系统是否能够导出指定范围内的数据，且数据符合预期
     * @throws Exception 
     */
    @Test
    public void testCustomRangeBeginAndEndAndSplit() throws Exception {
        Map<String, String> lines = getLines();
        lines.put("range", "'range':{"
                + "'begin':[{'type':'string', 'value':'000040'}], "
                + "'end':  [{'type':'string', 'value':'000140'}],"
                + "'split':[{'type':'string', 'value':'000050'},{'type':'string', 'value':'000080'},{'type':'string', 'value':'000100'},{'type':'string', 'value':'000130'}]}");
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
    
    /**
     * 测试目的：在用户指定参数的情况，观察系统是否符合预期
     * 测试内容：同时配置begin、split，期望程序将导入拆分为多个子任务，测试系统是否能够导出指定范围内的数据，且数据符合预期
     * @throws Exception 
     */
    @Test
    public void testCustomRangeBeginAndSplit() throws Exception {
        Map<String, String> lines = getLines();
        lines.put("range", "'range':{"
                + "'begin':[{'type':'string', 'value':'000040'}], "
                + "'split':[{'type':'string', 'value':'000050'},{'type':'string', 'value':'000080'},{'type':'string', 'value':'000100'},{'type':'string', 'value':'000130'}]}");
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
    
    /**
     * 测试目的：在用户指定参数的情况，观察系统是否符合预期
     * 测试内容：同时配置end、split，期望程序将导入拆分为多个子任务，测试系统是否能够导出指定范围内的数据，且数据符合预期
     * @throws Exception 
     */
    @Test
    public void testCustomRangeEndAndSplit() throws Exception {
        Map<String, String> lines = getLines();
        lines.put("range", "'range':{"
                + "'end':  [{'type':'string', 'value':'000140'}],"
                + "'split':[{'type':'string', 'value':'000050'},{'type':'string', 'value':'000080'},{'type':'string', 'value':'000100'},{'type':'string', 'value':'000130'}]}");
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
    
    /**
     * 测试目的：在用户指定参数的情况，观察系统是否符合预期
     * 测试内容：同时配置begin、end、split，且begin > end，期望程序将导入拆分为多个子任务，测试系统是否能够导出指定范围内的数据，且数据符合预期
     * @throws Exception 
     */
    @Test
    public void testCustomReverseRangeBeginAndEndAndSplit() throws Exception {
        Map<String, String> lines = getLines();
        lines.put("range", "'range':{"
                + "'begin':[{'type':'string', 'value':'000140'}], "
                + "'end':  [{'type':'string', 'value':'000001'}],"
                + "'split':[{'type':'string', 'value':'000130'},{'type':'string', 'value':'000100'},{'type':'string', 'value':'000080'},{'type':'string', 'value':'000030'}]}");
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
}
