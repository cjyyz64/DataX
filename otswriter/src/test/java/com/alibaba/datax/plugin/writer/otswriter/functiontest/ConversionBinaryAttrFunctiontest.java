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
 * 主要是测试各种类型转换为Binary的行为
 */
public class ConversionBinaryAttrFunctiontest {
    
    public static String tableName = "ots_writer_conversion_binary_attr_ft";
    public static BaseTest base = new BaseTest(tableName);
    public static List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
    public static List<ColumnType> attr = new ArrayList<ColumnType>();
    
    @BeforeClass
    public static void createTable() {
        pk.add(PrimaryKeyType.INTEGER);
        
        attr.add(ColumnType.BINARY);
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
    
    // 传入值是String，用户指定的是Binary, 期待转换正常，且值符合预期
    @Test
    public void testStringToBinary() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new StringColumn("在0.12.1版本及其以上的chunkserver 热升级中，chunkserver的suicide不会等到数据落盘退出，在使用mem log file场景下会造成数据丢失，该bug已经ci，但飞天sdk还未发出 请SLS也关注此bug。"));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(-1));
            row.put("attr_0", ColumnValue.fromBinary("在0.12.1版本及其以上的chunkserver 热升级中，chunkserver的suicide不会等到数据落盘退出，在使用mem log file场景下会造成数据丢失，该bug已经ci，但飞天sdk还未发出 请SLS也关注此bug。".getBytes("UTF-8")));
            expect.add(row);
        }
        test(rs, expect);
    }
    // 传入值是Int，用户指定的是Binary, 期待转换异常，异常信息符合预期
    @Test
    public void testIntToBinary() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new LongColumn(999));
            rs.add(r);
        }
        testIllegal(rs, new ArrayList<Record>(rs));
    }
    // 传入值是Double，用户指定的是Binary, 期待转换异常，异常信息符合预期
    @Test
    public void testDoubleToBinary() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new DoubleColumn(111.0));
            rs.add(r);
        }
        testIllegal(rs, new ArrayList<Record>(rs));
    }
    // 传入值是Bool，用户指定的是Binary, 期待转换异常，异常信息符合预期
    @Test
    public void testBoolToBinary() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new BoolColumn(true));
            rs.add(r);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new BoolColumn(false));
            rs.add(r);
        }
        testIllegal(rs, new ArrayList<Record>(rs));
    }
    // 传入值是Binary，用户指定的是Binary, 期待转换正常，且值符合预期
    @Test
    public void testBinaryToBinary() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new BytesColumn("we4=-t-=gtatrgtaewgrt".getBytes("UTF-8")));
            rs.add(r);
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(-1));
            row.put("attr_0", ColumnValue.fromBinary("we4=-t-=gtatrgtaewgrt".getBytes("UTF-8")));
            expect.add(row);
        }
        test(rs, expect);
    }
}
