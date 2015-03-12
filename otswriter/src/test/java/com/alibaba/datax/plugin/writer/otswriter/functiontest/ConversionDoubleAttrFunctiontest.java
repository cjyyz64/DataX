package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
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
 * 主要是测试各种类型转换为Double的行为
 */
public class ConversionDoubleAttrFunctiontest {
    
    public static String tableName = "ots_writer_conversion_double_attr_ft";
    public static BaseTest base = new BaseTest(tableName);
    public static List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
    public static List<ColumnType> attr = new ArrayList<ColumnType>();
    
    @BeforeClass
    public static void createTable() {
        pk.add(PrimaryKeyType.STRING);
        
        attr.add(ColumnType.DOUBLE);
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
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        test(input, collector, configuration, conf);
        assertEquals(0, collector.getContent().size());
        assertEquals(true, Utils.checkInput(base.getOts(), conf, expect));
    }
    
    private static void testIllegal(List<Record> input, List<Record> expect) throws Exception {
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.PUT_ROW);
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        test(input, collector, configuration, conf);
        assertEquals(true, Utils.checkInput(expect, collector.getRecord()));
    }
    
    // 传入值是String，用户指定的是Double, 期待转换正常，且值符合预期
    // -100, -23.1, 0, 43.01, 111
    @Test
    public void testStringToDouble() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        // 传入值是String
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("-100"));
            r.addColumn(new StringColumn("-100"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("-100"));
            row.put("attr_0", ColumnValue.fromDouble(-100.0));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("-23.1"));
            r.addColumn(new StringColumn("-23.1"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("-23.1"));
            row.put("attr_0", ColumnValue.fromDouble(-23.1));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new StringColumn("0"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("0"));
            row.put("attr_0", ColumnValue.fromDouble(0.0));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("43.01"));
            r.addColumn(new StringColumn("43.01"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("43.01"));
            row.put("attr_0", ColumnValue.fromDouble(43.01));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("111"));
            r.addColumn(new StringColumn("111"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("111"));
            row.put("attr_0", ColumnValue.fromDouble(111.0));
            expect.add(row);
        }
        test(rs, expect);
    }
    // 传入值是String，但是是非数值型的字符串，如“world”， “100L”， “0xff”，用户指定的是Double, 期待转换异常，异常信息符合预期
    @Test
    public void testIllegalStringToDouble() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("world"));
            r.addColumn(new StringColumn("world"));
            rs.add(r);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("100L"));
            r.addColumn(new StringColumn("100L"));
            rs.add(r);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0xff"));
            r.addColumn(new StringColumn("0xff"));
            rs.add(r);
        }
        testIllegal(rs, new ArrayList<Record>(rs));
    }
    // 传入值是Int，用户指定的是Double, 期待转换正常，且值符合预期
    // Long.min, Long.max, 0
    @Test
    public void testIntToDouble() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_1"));
            r.addColumn(new LongColumn(Long.MIN_VALUE));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("row_1"));
            row.put("attr_0", ColumnValue.fromDouble(Long.MIN_VALUE));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_2"));
            r.addColumn(new LongColumn(Long.MAX_VALUE));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("row_2"));
            row.put("attr_0", ColumnValue.fromDouble(Long.MAX_VALUE));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_3"));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("row_3"));
            row.put("attr_0", ColumnValue.fromDouble(0));
            expect.add(row);
        }
        test(rs, expect);
    }
    // 传入值是Double，用户指定的是Double, 期待转换正常，且值符合预期
    // Double.min, Double.max, 0
    @Test
    public void testDoubleToDouble() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_1"));
            r.addColumn(new DoubleColumn(Double.MIN_VALUE));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("row_1"));
            row.put("attr_0", ColumnValue.fromDouble(Double.MIN_VALUE));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_2"));
            r.addColumn(new DoubleColumn(Double.MAX_VALUE));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("row_2"));
            row.put("attr_0", ColumnValue.fromDouble(Double.MAX_VALUE));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_3"));
            r.addColumn(new DoubleColumn(0));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("row_3"));
            row.put("attr_0", ColumnValue.fromDouble(0));
            expect.add(row);
        }
        test(rs, expect);
    }
    // 传入值是Bool，用户指定的是Double, 期待转换正常，且值符合预期
    // true, false
    @Test
    public void testBoolToDouble() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_1"));
            r.addColumn(new BoolColumn(true));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("row_1"));
            row.put("attr_0", ColumnValue.fromDouble(1.0));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_2"));
            r.addColumn(new BoolColumn(false));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString("row_2"));
            row.put("attr_0", ColumnValue.fromDouble(0.0));
            expect.add(row);
        }
        test(rs, expect);
    }
    // 传入值是Binary，用户指定的是Double, 期待转换异常，异常信息符合预期
    @Test
    public void testBinaryToDouble() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_1"));
            r.addColumn(new BytesColumn("最低指数归一化双变量。它等于Math.getExponent返回的值".getBytes("UTF-8")));
            rs.add(r);
        }
        testIllegal(rs, new ArrayList<Record>(rs));
    }
}
