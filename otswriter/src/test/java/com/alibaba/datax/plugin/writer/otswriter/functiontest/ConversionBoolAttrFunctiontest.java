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
 * 主要是测试各种类型转换为Bool的行为
 */
public class ConversionBoolAttrFunctiontest {
    
    public static String tableName = "ots_writer_conversion_bool_attr_ft";
    public static BaseTest base = new BaseTest(tableName);
    public static List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
    public static List<ColumnType> attr = new ArrayList<ColumnType>();
    
    @BeforeClass
    public static void createTable() {
        pk.add(PrimaryKeyType.INTEGER);
        
        attr.add(ColumnType.BOOLEAN);
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
    
    // 传入值是String，用户指定的是Bool, 期待转换正常，且值符合预期
    // true, TRUE, TruE, false, FALSE, False
    @Test
    public void testStringToBool() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();

        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new StringColumn("true"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(-1));
            row.put("attr_0", ColumnValue.fromBoolean(true));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new StringColumn("TRUE"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(0));
            row.put("attr_0", ColumnValue.fromBoolean(true));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(1));
            r.addColumn(new StringColumn("TRUE"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(1));
            row.put("attr_0", ColumnValue.fromBoolean(true));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(2));
            r.addColumn(new StringColumn("false"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(2));
            row.put("attr_0", ColumnValue.fromBoolean(false));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(3));
            r.addColumn(new StringColumn("FALSE"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(3));
            row.put("attr_0", ColumnValue.fromBoolean(false));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(4));
            r.addColumn(new StringColumn("False"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(4));
            row.put("attr_0", ColumnValue.fromBoolean(false));
            expect.add(row);
        }
        test(rs, expect);
    }
    
    @Test
    public void testIllegalStringToBool() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new StringColumn("FFFFF"));
            rs.add(r);
        }
        testIllegal(rs, new ArrayList<Record>(rs));
    }
    
    // 传入值是Int，用户指定的是Bool, 期待转换正常，且值符合预期
    // -121, -1, 0, 1, 11
    @Test
    public void testIntToBool() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new LongColumn(-121));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(-1));
            row.put("attr_0", ColumnValue.fromBoolean(true));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new LongColumn(-1));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(0));
            row.put("attr_0", ColumnValue.fromBoolean(true));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(1));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(1));
            row.put("attr_0", ColumnValue.fromBoolean(false));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(2));
            r.addColumn(new LongColumn(1));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(2));
            row.put("attr_0", ColumnValue.fromBoolean(true));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(3));
            r.addColumn(new LongColumn(11));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(3));
            row.put("attr_0", ColumnValue.fromBoolean(true));
            expect.add(row);
        }
        test(rs, expect);
    }
    // 传入值是Double，用户指定的是Bool, 期待转换异常，异常信息符合预期
    // -1.0, 0.0, 1.0
    @Test
    public void testDoubleToBool() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new DoubleColumn(-1.0));
            rs.add(r);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new DoubleColumn(0.0));
            rs.add(r);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(1));
            r.addColumn(new DoubleColumn(1.0));
            rs.add(r);
        }
        testIllegal(rs, new ArrayList<Record>(rs));
    }
    // 传入值是Bool，用户指定的是Bool, 期待转换正常，且值符合预期
    // true, false
    @Test
    public void testBoolToBool() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new BoolColumn(true));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(-1));
            row.put("attr_0", ColumnValue.fromBoolean(true));
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new BoolColumn(false));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(0));
            row.put("attr_0", ColumnValue.fromBoolean(false));
            expect.add(row);
        }
        test(rs, expect);
    }
    // 传入值是Binary，用户指定的是Bool, 期待转换异常，异常信息符合预期
    @Test
    public void BinaryToBool() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new BytesColumn("try you best for succ".getBytes("UTF-8")));
            rs.add(r);
        }
        testIllegal(rs, new ArrayList<Record>(rs));
    }
}
