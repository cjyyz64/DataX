package com.alibaba.datax.plugin.writer.otswriter.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.OtsWriterSlaveProxy;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector.RecordAndMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.alibaba.datax.test.simulator.util.RecordReceiverForTest;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.ColumnType;
import com.aliyun.openservices.ots.internal.model.Row;

public class BaseTest {

    public void testWithTS(
            OTS ots,
            OTSConf conf, 
            List<Record> input, 
            List<Row> expect
            ) throws Exception {
        test(ots, conf, input, expect, null, true);
    }
    
    public void testWithNoTS(
            OTS ots,
            OTSConf conf, 
            List<Record> input, 
            List<Row> expect
            ) throws Exception {
        test(ots, conf, input, expect, null, false);
    }
    
    
    
    /**
     * 测试程序异常退出
     * @param ots
     * @param conf
     * @param input
     * @param errorMsg
     * @throws Exception
     */
    public void test(
            OTS ots,
            OTSConf conf, 
            List<Record> input, 
            String errorMsg
            ) throws Exception {
        Configuration configuration = Configuration.newDefault();
        configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(conf));
        RecordReceiverForTest recordReceiver = new RecordReceiverForTest(input);
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        OtsWriterSlaveProxy slave = new OtsWriterSlaveProxy();
        slave.init(configuration);
        try {
            slave.write(recordReceiver, collector);
            fail();
        } catch (Exception e) {
            assertEquals(errorMsg, e.getMessage());
        } finally {
            slave.close();
        }
    }
    
    /**
     * 测试脏数据回收器数据和OTS中的数据
     * @param ots
     * @param conf
     * @param input
     * @param expect
     * @param rm
     * @throws Exception
     */
    public void test(
            OTS ots,
            OTSConf conf, 
            List<Record> input,
            List<Row> expect,
            List<RecordAndMessage> rm,
            boolean isCheckTS
            ) throws Exception {
        Configuration configuration = Configuration.newDefault();
        configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(conf));
        RecordReceiverForTest recordReceiver = new RecordReceiverForTest(input);
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        OtsWriterSlaveProxy slave = new OtsWriterSlaveProxy();
        slave.init(configuration);
        try {
            slave.write(recordReceiver, collector);
        } finally {
            slave.close();
        }
        if (rm == null) {
            for (RecordAndMessage s : collector.getContent()) {
                System.out.println(s.toString());
            }
            assertEquals(0, collector.getContent().size());
        } else {
            assertEquals(rm.size(), collector.getContent().size());
            assertEquals(true, DataChecker.checkRecordWithMessage(collector.getContent(), rm));
        }
        if (expect != null) {
            assertEquals(true, DataChecker.checkRow(OTSHelper.getAllData(ots, conf), expect, isCheckTS));
        }
    }
    
    public String getColumnName(int index) {
        return String.format("attr_%06d", index);
    }
    
    public String getPKColumnName(int index) {
        return String.format("pk_%06d", index);
    }
    
    public Map<String, ColumnType> getColumnMeta(int count, ColumnType type) {
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        for (int i = 0; i < count; i++) {
            columns.put(getColumnName(i), type);
        }
        return columns;
    }
    
    public Map<String, ColumnType> getColumnMeta(int begin, int count, ColumnType type) {
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        for (int i = begin; i < begin + count; i++) {
            columns.put(getColumnName(i), type);
        }
        return columns;
    }
}
