package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Test;

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
 * 通过Put的方式导入数据到OTS中，验证数据的正确性
 */
public class ExportDataByPutFunctiontest {
    public static String tableName = "ots_writer_put_ft";
    public static BaseTest base = new BaseTest(tableName);
    
    @AfterClass
    public static void close() {
        base.close();
    }
    
    // 混合PK数据导入测试，主要测试Writer在各种PK组合下功能是否正常
    
    private static void test(List<Record> input, TestPluginCollector collector, Configuration configuration, OTSConf conf, int begin, int rowCount) throws Exception {
        base.prepareData(Utils.getPKTypeList(conf.getPrimaryKeyColumn()), begin, rowCount, 0);
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

    private static void test(List<PrimaryKeyType> pk, List<ColumnType> attr, List<Record> input, List<Map<String, ColumnValue>> expect) throws Exception {
        test(pk, attr, input, expect, 0, 0);
    }
    
    private static void test(List<PrimaryKeyType> pk, List<ColumnType> attr, List<Record> input, List<Map<String, ColumnValue>> expect, int begin, int rowCount) throws Exception {
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.PUT_ROW);
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        test(input, collector, configuration, conf, begin, rowCount);
        assertEquals(0, collector.getContent().size());
        assertEquals(true, Utils.checkInput(base.getOts(), conf, expect));
    }
    
    private static void testIllegal(List<PrimaryKeyType> pk, List<ColumnType> attr, List<Record> input, List<Record> expect) throws Exception {
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.PUT_ROW);
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        test(input, collector, configuration, conf, 0, 0);
        assertEquals(true, Utils.checkInput(expect, collector.getRecord()));
    }
    
    private void test(List<PrimaryKeyType> pk, List<ColumnType> attr, int begin, int rowCount) throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        for (int i = 0; i < 100; i++) {
            Record r = new DefaultRecord();
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            
            int pkIndex = 0;
            for (PrimaryKeyType t : pk) {
                r.addColumn(Utils.getPKColumn(t, i));
                row.put("pk_" + pkIndex, Utils.getPKColumnValue(t, i));
                pkIndex++;
            }
            
            int attrIndex = 0;
            for (ColumnType t : attr) {
                r.addColumn(Utils.getAttrColumn(t, i));
                row.put("attr_" + attrIndex, Utils.getAttrColumnValue(t, i));
                attrIndex++;
            }
            rs.add(r);
            expect.add(row);
        }
        
        test(pk, attr, rs, expect);
    }
   
    /**
     * 测试多种PK组合下的数据导入是否符合预期
     * @throws Exception
     */
    @Test
    public void testMultiPK() throws Exception {
        PrimaryKeyType [][] types = {
                {PrimaryKeyType.INTEGER},
                {PrimaryKeyType.STRING},
                
                {PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                {PrimaryKeyType.INTEGER, PrimaryKeyType.STRING},
                {PrimaryKeyType.STRING, PrimaryKeyType.INTEGER},
                {PrimaryKeyType.STRING, PrimaryKeyType.STRING},
                
                {PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                {PrimaryKeyType.STRING, PrimaryKeyType.STRING, PrimaryKeyType.STRING, PrimaryKeyType.STRING},
                {PrimaryKeyType.STRING, PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                {PrimaryKeyType.INTEGER, PrimaryKeyType.STRING, PrimaryKeyType.STRING, PrimaryKeyType.STRING}
        };
        
        List<ColumnType> attr = new ArrayList<ColumnType>();
        attr.add(ColumnType.STRING);
        attr.add(ColumnType.INTEGER);
        attr.add(ColumnType.DOUBLE);
        attr.add(ColumnType.BOOLEAN);
        attr.add(ColumnType.BINARY);
        
        for (PrimaryKeyType [] pkTypes : types) {
            List<PrimaryKeyType> pk = Arrays.asList(pkTypes);
            test(pk, attr, -100, 200);
        }
    }
    
    
    // 在导入的Record中，PK有空的Column
    @Test
    public void testNulPKlColumn() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        List<ColumnType> attr = new ArrayList<ColumnType>();
        attr.add(ColumnType.STRING);
        attr.add(ColumnType.INTEGER);
        
        List<Record> rs = new ArrayList<Record>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new StringColumn("-1"));
            r.addColumn(new StringColumn());
            r.addColumn(new StringColumn("-1"));
            
            r.addColumn(new StringColumn("-1"));
            r.addColumn(new LongColumn(-1));
            rs.add(r);
        }
        
        testIllegal(pk, attr, rs, new ArrayList<Record>(rs));
    }
    
    // 在导入的Record中，Attr中有空的Column
    @Test
    public void testNullAttrColumn() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        List<ColumnType> attr = new ArrayList<ColumnType>();
        attr.add(ColumnType.STRING);
        attr.add(ColumnType.INTEGER);
        
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new StringColumn("-1"));
            r.addColumn(new LongColumn(-1));
            r.addColumn(new StringColumn("-1"));
            
            r.addColumn(new StringColumn());
            r.addColumn(new LongColumn(-1));
            rs.add(r);
            
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromLong(-1));
            row.put("pk_1", ColumnValue.fromString("-1"));
            row.put("pk_2", ColumnValue.fromLong(-1));
            row.put("pk_3", ColumnValue.fromString("-1"));
            
            //row.put("attr_0", null);
            row.put("attr_1", ColumnValue.fromLong(-1));
            expect.add(row);
        }
        
        test(pk, attr, rs, expect);
    }
    
    // 原表中已经存在了100行数据，重新导入100行数据，新导入的数据只有50行数据和原表的PK一致，期望结果符合预期
    @Test
    public void testHadData() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        List<ColumnType> attr = new ArrayList<ColumnType>();
        attr.add(ColumnType.STRING);
        attr.add(ColumnType.INTEGER);
        
        List<Record> rs = new ArrayList<Record>();
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();
        
        for (int i = -50; i < 0; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(String.valueOf(i)));
            r.addColumn(new StringColumn(String.valueOf(i)));
            r.addColumn(new LongColumn(i));
            r.addColumn(new LongColumn(i));

            r.addColumn(new StringColumn(String.valueOf(i)));
            r.addColumn(new LongColumn(i));
            rs.add(r);
            
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString(String.valueOf(i)));
            row.put("pk_1", ColumnValue.fromString(String.valueOf(i)));
            row.put("pk_2", ColumnValue.fromLong(i));
            row.put("pk_3", ColumnValue.fromLong(i));
            
            row.put("attr_0", ColumnValue.fromString(String.valueOf(i)));
            row.put("attr_1", ColumnValue.fromLong(i));
            expect.add(row);
        }
        
        for (int i = 0; i < 50; i++) {
            Map<String, ColumnValue> row = new LinkedHashMap<String, ColumnValue>();
            row.put("pk_0", ColumnValue.fromString(String.valueOf(i)));
            row.put("pk_1", ColumnValue.fromString(String.valueOf(i)));
            row.put("pk_2", ColumnValue.fromLong(i));
            row.put("pk_3", ColumnValue.fromLong(i));
            
            row.put("attr_0", ColumnValue.fromString(String.valueOf(i)));
            row.put("attr_1", ColumnValue.fromLong(i));
            expect.add(row);
        }
        
        test(pk, attr, rs, expect, -50, 100);
    }
}
