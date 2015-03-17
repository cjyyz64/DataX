package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
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

public class CUFunctiontest {

    private static String tableName = "ots_writer_cu_test";

    private static BaseTest base = new BaseTest(tableName);

    @AfterClass
    public static void close() {
        base.close();
    }

    private List<Record> buildRecord(List<PrimaryKeyType> pkTypes, long begin, long rowCount, int cellSize) {
        List<Record> rs = new ArrayList<Record>();

        for (long i = begin; i < (begin + rowCount); i++) {
            Record r = new DefaultRecord();
            for (int j = 0; j < pkTypes.size(); j++) {
                PrimaryKeyType type = pkTypes.get(j);
                if (type == PrimaryKeyType.INTEGER) {
                    r.addColumn(new LongColumn(i));
                } else {
                    r.addColumn(new StringColumn(String.valueOf(i)));
                }
            }

            StringBuilder attr = new StringBuilder();
            for(int j = 0; j < cellSize; j++) {
                attr.append("a");
            }
            r.addColumn(new StringColumn(attr.toString()));
            rs.add(r);
        }
        return rs;
    }
    
    private List<Map<String, ColumnValue>> buildRow(List<PrimaryKeyType> pkTypes, long begin, long rowCount, int cellSize) {
        List<Map<String, ColumnValue>> expect = new ArrayList<Map<String, ColumnValue>>();

        for (long i = begin; i < (begin + rowCount); i++) {
            Map<String, ColumnValue> r = new LinkedHashMap<String, ColumnValue>();
            for (int j = 0; j < pkTypes.size(); j++) {
                PrimaryKeyType type = pkTypes.get(j);
                if (type == PrimaryKeyType.INTEGER) {
                    r.put("pk_" + j, ColumnValue.fromLong(i));
                } else {
                    r.put("pk_" + j, ColumnValue.fromString(String.valueOf(i)));
                }
            }

            StringBuilder attr = new StringBuilder();
            for(int j = 0; j < cellSize; j++) {
                attr.append("a");
            }
            r.put("attr_0", ColumnValue.fromString(attr.toString()));
            expect.add(r);
        }
        return expect;
    }

    /**
     * 输入：创建一个1CU的表，构造100行数据，每行数据都大于1CU，并将数据导入到OTS中
     * 期望：数据能够被全部导入到OTS中，且数据正确
     * @throws Exception 
     */
    @Test
    public void testNoEnoughCUForAllRow() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        List<ColumnType> attr = new ArrayList<ColumnType>();
        attr.add(ColumnType.STRING);
        
        base.createTable(pk, 5000, 1);

        List<Record> records = buildRecord(pk, -1, 100, 1025);
        List<Map<String, ColumnValue>> expect = buildRow(pk, -1, 100, 1025);

        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.PUT_ROW);
        conf.setRetry(18);
        conf.setConcurrencyWrite(1);
        conf.setBatchWriteCount(1);
        
        Configuration configuration = Configuration.newDefault();
        configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(conf));
        RecordReceiverForTest recordReceiver = new RecordReceiverForTest(records);
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        OtsWriterSlaveProxy slave = new OtsWriterSlaveProxy();

        slave.init(configuration);
        try {
            slave.write(recordReceiver, collector);
        } finally {
            slave.close();
        }
        assertEquals(0, collector.getContent().size());
        assertEquals(true, Utils.checkInput(base.getOts(), conf, expect));
    }
}
