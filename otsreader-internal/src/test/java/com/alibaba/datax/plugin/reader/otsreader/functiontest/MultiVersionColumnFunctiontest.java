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


public class MultiVersionColumnFunctiontest {
    
    private static String tableName = "multiVersion";
    private static Configuration p = ConfigurationHelper.loadConf();
    private static OTS ots = null;
    
    @BeforeClass
    public static void setBeforeClass() throws Exception {
        ots = OtsHelper.getOTSInstance();
        
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("Pid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("Uid", PrimaryKeyType.STRING);
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
        lines.put("mode",         "'mode':'multiVersion'");
        return lines;
    }
    
    /**
     * 测试目的：测试在默认参数的条件下，系统导出的Column是否正确
     * 测试内容：Column不填，期望导出所有的列，且值符合预期
     * @throws Exception 
     */
    @Test
    public void testDefaultColumn() throws Exception {
        Map<String, String> lines = getLines();
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
    
    /**
     * 测试目的：测试在指定列的情况下，系统导出的Column是否正确
     * 测试内容：Column中指定10个存在的列，10个不存在的列，期望导出指定的列，且值符合预期
     * @throws Exception 
     */
    @Test
    public void testNormalColumn() throws Exception {
        Map<String, String> lines = getLines();
        lines.put("column",         "'column':["
                + "{'name':'col_000000'},{'name':'col_000001'},{'name':'col_000002'},{'name':'col_000003'},{'name':'col_000004'},"
                + "{'name':'col_000005'},{'name':'col_000006'},{'name':'col_000007'},{'name':'col_000008'},{'name':'col_000009'},"
                + "{'name':'null_000000'},{'name':'null_000001'},{'name':'null_000002'},{'name':'null_000003'},{'name':'null_000004'},"
                + "{'name':'null_000005'},{'name':'null_000006'},{'name':'null_000007'},{'name':'null_000008'},{'name':'null_000009'},"
                + "]");
        Configuration c = Configuration.from(ConfigurationHelper.linesToJson(lines));
        TestHelper.test(c);
    }
}
