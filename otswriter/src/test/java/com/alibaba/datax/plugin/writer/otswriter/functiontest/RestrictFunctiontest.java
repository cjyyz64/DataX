package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.otswriter.OtsWriterSlaveProxy;
import com.alibaba.datax.plugin.writer.otswriter.common.BaseTest;
import com.alibaba.datax.plugin.writer.otswriter.common.Conf;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.common.Utils;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.alibaba.datax.test.simulator.util.RecordReceiverForTest;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyType;

/**
 * 限制项相关的测试
 */
public class RestrictFunctiontest {
    
    private static String tableName = "ots_writer_restrict_ft";
    private static BaseTest base = new BaseTest(tableName);
    private static List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
    private static List<ColumnType> attr = new ArrayList<ColumnType>();
    
    @BeforeClass
    public static void createTable() {
        pk.add(PrimaryKeyType.STRING);
        attr.add(ColumnType.STRING);
        attr.add(ColumnType.BINARY);
        for (int i = 2; i < 20; i++) {
            attr.add(ColumnType.STRING);
        }
    }
    
    @AfterClass
    public static void close() {
        base.close();
    }
    
    private static void test(List<Record> input, TestPluginCollector collector, Configuration configuration, OTSConf conf) throws Exception {
        base.createTable(pk);
        configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(conf));
        RecordReceiverForTest recordReceiver = new RecordReceiverForTest(input);
        OtsWriterSlaveProxy slave = new OtsWriterSlaveProxy();
        slave.init(configuration);
        try {
            slave.write(recordReceiver, collector);
        } finally {
            slave.close();
        }
    }

    private static void test(List<Record> input, List<Map<String, ColumnValue>> expect) throws Exception {
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.PUT_ROW);
        conf.setBatchWriteCount(100);
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        test(input, collector, configuration, conf);
        assertEquals(0, collector.getContent().size());
        assertEquals(true, Utils.checkInput(base.getOts(), conf, expect));
    }
    
    private static void testIllegal(List<Record> input, List<Record> expect) throws Exception {
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.PUT_ROW);
        conf.setBatchWriteCount(100);
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        test(input, collector, configuration, conf);
        assertEquals(true, Utils.checkInput(expect, collector.getRecord()));
    }
    
    // PK的String Column的值等于1KB
    @Test
    public void testPKString1024B() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1024; i++) {
            sb.append("c");
        }
        
        {
            Record r = new DefaultRecord();
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes("UTF-8")));
            
            row.put("pk_0", ColumnValue.fromString(sb.toString()));
            row.put("attr_0", ColumnValue.fromString("0"));
            row.put("attr_1", ColumnValue.fromBinary("0".getBytes("UTF-8")));
            
            for (int i = 2; i < 20; i++) {
                r.addColumn(new StringColumn("0"));
                row.put("attr_" + i, ColumnValue.fromString("0"));
            }
            rs.add(r);
            expect.add(row);
        }
        test(rs, expect);
    }
    // PK的String Column的值大于1KB
    @Test
    public void testPKString1025B() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1025; i++) {
            sb.append("c");
        }
        
        {
            Record r = new DefaultRecord();
            
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes("UTF-8")));
            
            for (int i = 2; i < 20; i++) {
                r.addColumn(new StringColumn("0"));
            }
            rs.add(r);
        }
        testIllegal(rs, new ArrayList<Record>(rs));
    }
    // Attr的String Column的值等于64KB
    @Test
    public void testAttrString64KB() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 64*1024; i++) {
            sb.append("a");
        }
        
        {
            Record r = new DefaultRecord();
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            
            r.addColumn(new StringColumn("0"));
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new BytesColumn("0".getBytes("UTF-8")));
            
            row.put("pk_0", ColumnValue.fromString("0"));
            row.put("attr_0", ColumnValue.fromString(sb.toString()));
            row.put("attr_1", ColumnValue.fromBinary("0".getBytes("UTF-8")));
            
            for (int i = 2; i < 20; i++) {
                r.addColumn(new StringColumn("0"));
                row.put("attr_" + i, ColumnValue.fromString("0"));
            }
            rs.add(r);
            expect.add(row);
        }
        test(rs, expect);
    }
    // Attr的String Column的值大于64KB
    @Test
    public void testAttrStringMoreThan64KB() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (64*1024+1); i++) {
            sb.append("a");
        }
        
        {
            Record r = new DefaultRecord();
            
            r.addColumn(new StringColumn("0"));
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new BytesColumn("0".getBytes("UTF-8")));
            
            for (int i = 2; i < 20; i++) {
                r.addColumn(new StringColumn("0"));
            }
            rs.add(r);
        }
        testIllegal(rs, new ArrayList<Record>(rs));
    }
    // Binary Column的值等于64KB
    @Test
    public void testAttrBinary64KB() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 64*1024; i++) {
            sb.append("a");
        }
        
        {
            Record r = new DefaultRecord();
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            
            r.addColumn(new StringColumn("0"));
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn(sb.toString().getBytes("UTF-8")));
            
            row.put("pk_0", ColumnValue.fromString("0"));
            row.put("attr_0", ColumnValue.fromString("0"));
            row.put("attr_1", ColumnValue.fromBinary(sb.toString().getBytes("UTF-8")));
            
            for (int i = 2; i < 20; i++) {
                r.addColumn(new StringColumn("0"));
                row.put("attr_" + i, ColumnValue.fromString("0"));
            }
            rs.add(r);
            expect.add(row);
        }
        test(rs, expect);
    }
    // Binary Column的值大于64KB
    @Test
    public void testAttrBinaryMoreThan64KB() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (64*1024+1); i++) {
            sb.append("a");
        }
        
        {
            Record r = new DefaultRecord();
            
            r.addColumn(new StringColumn("0"));
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn(sb.toString().getBytes("UTF-8")));
            
            for (int i = 2; i < 20; i++) {
                r.addColumn(new StringColumn("0"));
            }
            rs.add(r);
        }
        testIllegal(rs, new ArrayList<Record>(rs));
    }
    // 单个请求等于1MB
    @Test
    public void testRequest1024KB() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 64*1024; i++) {
            sb.append("r");
        }
        
        for (int j = 0; j < 20; j++) {
            Record r = new DefaultRecord();
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            
            String s = String.valueOf(j);
            // record
            r.addColumn(new StringColumn(s));
            r.addColumn(new StringColumn(s));
            r.addColumn(new BytesColumn(sb.toString().getBytes("UTF-8")));
            r.addColumn(new StringColumn());
            r.addColumn(new StringColumn());
            r.addColumn(new StringColumn());
            
            // expect
            row.put("pk_0", ColumnValue.fromString(s));
            row.put("attr_0", ColumnValue.fromString(s));
            row.put("attr_1", ColumnValue.fromBinary(sb.toString().getBytes("UTF-8")));
            
            for (int i = 5; i < 20; i++) {
                r.addColumn(new StringColumn("0"));
                row.put("attr_" + i, ColumnValue.fromString("0"));
            }
            
            rs.add(r);
            expect.add(row);
        }
        
        test(rs, expect);
    }
}
