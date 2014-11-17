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
 * 主要是测试各种类型转换为Int的行为
 */
public class ConversionIntAttrFunctiontest {
    
    public static String tableName = "ots_writer_conversion_int_attr_ft";
    public static BaseTest base = new BaseTest(tableName);
    public static List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
    public static List<ColumnType> atrr = new ArrayList<ColumnType>();
    
    
    @BeforeClass
    public static void createTable() {
        pk.add(PrimaryKeyType.INTEGER);
        atrr.add(ColumnType.INTEGER);
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
        OTSConf conf = Conf.getConf(tableName, pk, atrr, OTSOpType.PUT_ROW);
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        test(input, collector, configuration, conf);
        assertEquals(0, collector.getContent().size());
        assertEquals(true, Utils.checkInput(base.getOts(), conf, expect));
    }
    
    private static void testIllegal(List<Record> input, List<Record> expect) throws Exception {
        OTSConf conf = Conf.getConf(tableName, pk, atrr, OTSOpType.PUT_ROW);
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        test(input, collector, configuration, conf);
        assertEquals(true, Utils.checkInput(expect, collector.getRecord()));
    }

    
    /**
     * 传入 : 值是String，用户指定的是Int
     * 期待 : 转换正常，且值符合预期
     * @throws Exception
     */
    @Test
    public void testStringToInt() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        // 传入值是String
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("-100"));
            r.addColumn(new StringColumn("-100"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(-100));
            row.put("attr_0", ColumnValue.fromLong(-100));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("10E2"));
            r.addColumn(new StringColumn("10E2"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(1000));
            row.put("attr_0", ColumnValue.fromLong(1000));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new StringColumn("0"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(0));
            row.put("attr_0", ColumnValue.fromLong(0));
            expect.add(row);
        }
        
        test(rs, expect);
    }
    
    /**
     * 传入值是Int，用户指定的是Int, 期待转换正常，且值符合预期
     * @throws Exception
     */
    @Test
    public void testIntToInt() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        // 传入值是Int
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(Long.MIN_VALUE));
            r.addColumn(new LongColumn(Long.MIN_VALUE));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(Long.MIN_VALUE));
            row.put("attr_0", ColumnValue.fromLong(Long.MIN_VALUE));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(Long.MAX_VALUE - 1));
            r.addColumn(new LongColumn(Long.MAX_VALUE - 1));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(Long.MAX_VALUE - 1));
            row.put("attr_0", ColumnValue.fromLong(Long.MAX_VALUE - 1));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(0));
            row.put("attr_0", ColumnValue.fromLong(0));
            expect.add(row);
        }
        
        test(rs, expect);
    }
    
    /**
     * 传入值是Double，用户指定的是Int, 期待转换正常，且值符合预期
     * @throws Exception
     */
    @Test
    public void testDoubleToInt() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        // 传入值是Double
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(-9012.023));
            r.addColumn(new DoubleColumn(-9012.023));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(-9012));
            row.put("attr_0", ColumnValue.fromLong(-9012));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(0));
            r.addColumn(new DoubleColumn(0));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(0));
            row.put("attr_0", ColumnValue.fromLong(0));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(1211.12));
            r.addColumn(new DoubleColumn(1211.12));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(1211));
            row.put("attr_0", ColumnValue.fromLong(1211));
            expect.add(row);
        }
        
        test(rs, expect);
    }
    
    /**
     *  传入值是Bool，用户指定的是String, 期待转换正常，且值符合预期
     * @throws Exception
     */
    @Test
    public void testBoolToInt() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new BoolColumn(true));
            r.addColumn(new BoolColumn(true));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(1));
            row.put("attr_0", ColumnValue.fromLong(1));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new BoolColumn(false));
            r.addColumn(new BoolColumn(false));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(0));
            row.put("attr_0", ColumnValue.fromLong(0));
            expect.add(row);
        }
        
        test(rs, expect);
    }

    /**
     * 用户输入Int，但是系统传入非数值型的字符串，期望写入OTS失败
     * 例如："", "hello", "0x5f", "100L"
     * @throws Exception
     */
    @Test
    public void testIllegalStringToInt() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(""));
            r.addColumn(new StringColumn(""));
            rs.add(r);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello"));
            r.addColumn(new StringColumn("hello"));
            rs.add(r);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0x5f"));
            r.addColumn(new StringColumn("0x5f"));
            rs.add(r);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("100L"));
            r.addColumn(new StringColumn("100L"));
            rs.add(r);
        }
        testIllegal(rs, new ArrayList<Record>(rs));
    }
    
    /**
     * 传入Binary，期望写入OTS失败
     * @throws Exception
     */
    @Test
    public void testBinaryToInt() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new BytesColumn("！@#￥%……&*（）——+“：{}？》《".getBytes("UTF-8")));
            r.addColumn(new BytesColumn("！@#￥%……&*（）——+“：{}？》《".getBytes("UTF-8")));
            rs.add(r);
        }
        testIllegal(rs, new ArrayList<Record>(rs));
    }
}
