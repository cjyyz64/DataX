package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderMasterProxy;
import com.alibaba.datax.plugin.reader.otsreader.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsreader.common.OtsHelper;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSMode;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;
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
    
    private Map<String, String> getNormalLines() {
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
        lines.put("mode",         "'mode':'normal'");
        lines.put("column",       "'column': [{'value':'value', 'type':'string'}]");
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
        String json = linesToJson(this.getNormalLines());
        Configuration configuration = Configuration.from(json);
        
        proxy.init(configuration);
        
        OTSConf conf = proxy.getConf();
        
        assertEquals(p.getString("endpoint"), conf.getEndpoint());
        assertEquals(p.getString("accessid"), conf.getAccessId());
        assertEquals(p.getString("accesskey"), conf.getAccessKey());
        assertEquals(p.getString("instance-name"), conf.getInstanceName());
        assertEquals(tableName, conf.getTableName());
        assertEquals(OTSMode.NORMAL, conf.getMode());
    }
}
