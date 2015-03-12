package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
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
 * 主要是测试各种类型转换为String的行为
 */
public class ConversionStringPKFunctiontest {
   
    public static String tableName = "ots_writer_conversion_string_pk_ft";
    public static BaseTest base = new BaseTest(tableName);
    public static List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
    
    @BeforeClass
    public static void createTable() {
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
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
        OTSConf conf = Conf.getConf(tableName, pk, Collections.<ColumnType> emptyList(), OTSOpType.PUT_ROW);
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        test(input, collector, configuration, conf);
        assertEquals(0, collector.getContent().size());
        assertEquals(true, Utils.checkInput(base.getOts(), conf, expect));
    }
    
    /**
     * 传入 : 值是String，用户指定的是String 
     * 期待 : 转换正常，且值符合预期
     * @throws Exception
     */
    @Test
    public void testStringToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        // 传入值是String
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(""));
            r.addColumn(new StringColumn(""));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString(""));
            row.put("pk_1", ColumnValue.fromString(""));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("-100"));
            r.addColumn(new StringColumn("-100"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("-100"));
            row.put("pk_1", ColumnValue.fromString("-100"));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("100L"));
            r.addColumn(new StringColumn("100L"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("100L"));
            row.put("pk_1", ColumnValue.fromString("100L"));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0x5f"));
            r.addColumn(new StringColumn("0x5f"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("0x5f"));
            row.put("pk_1", ColumnValue.fromString("0x5f"));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new StringColumn("0"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("0"));
            row.put("pk_1", ColumnValue.fromString("0"));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("(*^__^*) 嘻嘻……"));
            r.addColumn(new StringColumn("(*^__^*) 嘻嘻……"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("(*^__^*) 嘻嘻……"));
            row.put("pk_1", ColumnValue.fromString("(*^__^*) 嘻嘻……"));
            expect.add(row);
        }
        
        test(rs, expect);
    }
    
    
    // 传入值是Int，用户指定的是String, 期待转换正常，且值符合预期
    @Test
    public void testIntToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        // 传入值是Int
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(Long.MIN_VALUE));
            r.addColumn(new LongColumn(Long.MIN_VALUE));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString(String.valueOf(Long.MIN_VALUE)));
            row.put("pk_1", ColumnValue.fromString(String.valueOf(Long.MIN_VALUE)));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(Long.MAX_VALUE));
            r.addColumn(new LongColumn(Long.MAX_VALUE));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString(String.valueOf(Long.MAX_VALUE)));
            row.put("pk_1", ColumnValue.fromString(String.valueOf(Long.MAX_VALUE)));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("0"));
            row.put("pk_1", ColumnValue.fromString("0"));
            expect.add(row);
        }
        
        test(rs, expect);
    }
    
    // 传入值是Double，用户指定的是String, 期待转换正常，且值符合预期
    @Test
    public void testDoubleToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        // 传入值是Double
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(-9012.023));
            r.addColumn(new DoubleColumn(-9012.023));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("-9012.023"));
            row.put("pk_1", ColumnValue.fromString("-9012.023"));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(0));
            r.addColumn(new DoubleColumn(0));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("0"));
            row.put("pk_1", ColumnValue.fromString("0"));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(1211.12));
            r.addColumn(new DoubleColumn(1211.12));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("1211.12"));
            row.put("pk_1", ColumnValue.fromString("1211.12"));
            expect.add(row);
        }
        
        test(rs, expect);
    }
    // 传入值是Bool，用户指定的是String, 期待转换正常，且值符合预期
    @Test
    public void testBoolToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new BoolColumn(true));
            r.addColumn(new BoolColumn(true));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("true"));
            row.put("pk_1", ColumnValue.fromString("true"));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new BoolColumn(false));
            r.addColumn(new BoolColumn(false));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("false"));
            row.put("pk_1", ColumnValue.fromString("false"));
            expect.add(row);
        }
        
        test(rs, expect);
    }
    // 传入值是Binary，用户指定的是String, 期待转换正常，且值符合预期
    @Test
    public void testBinaryToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new BytesColumn("试试~，。1？！@#￥%……&*（）——+".getBytes("UTF-8")));
            r.addColumn(new BytesColumn("试试~，。1？！@#￥%……&*（）——+".getBytes("UTF-8")));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("试试~，。1？！@#￥%……&*（）——+"));
            row.put("pk_1", ColumnValue.fromString("试试~，。1？！@#￥%……&*（）——+"));
            expect.add(row);
        }
        
        test(rs, expect);
    }
}
