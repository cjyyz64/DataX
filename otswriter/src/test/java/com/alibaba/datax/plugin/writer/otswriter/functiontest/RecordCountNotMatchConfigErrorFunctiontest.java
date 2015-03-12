package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

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
import com.alibaba.datax.plugin.writer.otswriter.common.Table;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSPKColumn;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.alibaba.datax.test.simulator.util.RecordReceiverForTest;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.PrimaryKeyType;

/**
 * 测试当传入的Record的Column的长度和用户配置的Column长度不一致时的行为
 */
public class RecordCountNotMatchConfigErrorFunctiontest{
    
    private static String tableName = "ots_writer_record_count_not_match_config_error";
    
    private static BaseTest base = new BaseTest(tableName);
    
    private static List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
    public static List<ColumnType> attr = new ArrayList<ColumnType>();
    
    @BeforeClass
    public static void prepare() throws Exception {
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        
        attr.add(ColumnType.STRING);
        attr.add(ColumnType.INTEGER);
        attr.add(ColumnType.DOUBLE);
        
        Table t = new Table(base.getOts(), tableName, pk);
        t.create(5000, 5000);
    }
    
    @AfterClass
    public static void close() {
        base.close();
    }
    
    public Configuration getConf() {
        Configuration configuration = Configuration.newDefault();
        Configuration p = base.getP();
        
        OTSConf conf = new OTSConf();
        conf.setEndpoint(p.getString("endpoint"));
        conf.setAccessId(p.getString("accessid"));
        conf.setAccessKey(p.getString("accesskey"));
        conf.setInstanceName(p.getString("instance-name"));
        conf.setTableName(tableName);
        
        List<OTSPKColumn> primaryKeyColumn = new ArrayList<OTSPKColumn>();
        primaryKeyColumn.add(new OTSPKColumn("pk_0", PrimaryKeyType.STRING));
        primaryKeyColumn.add(new OTSPKColumn("pk_1", PrimaryKeyType.INTEGER));
        primaryKeyColumn.add(new OTSPKColumn("pk_2", PrimaryKeyType.INTEGER));
        primaryKeyColumn.add(new OTSPKColumn("pk_3", PrimaryKeyType.INTEGER));
        conf.setPrimaryKeyColumn(primaryKeyColumn);
        
        List<OTSAttrColumn> attributeColumn = new ArrayList<OTSAttrColumn>();
        attributeColumn.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attributeColumn.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        attributeColumn.add(new OTSAttrColumn("attr_2", ColumnType.DOUBLE));
        attributeColumn.add(new OTSAttrColumn("attr_3", ColumnType.BOOLEAN));
        attributeColumn.add(new OTSAttrColumn("attr_4", ColumnType.BINARY));
        
        conf.setAttributeColumn(attributeColumn);
        
        conf.setOperation(OTSOpType.UPDATE_ROW);
        
        conf.setRetry(1);
        conf.setSleepInMilliSecond(100);
        conf.setBatchWriteCount(5);
        conf.setConcurrencyWrite(5);
        
        configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(conf));
        
        return configuration;
    }
    
    /**
     * 输入：构造每个Record中有6列数据，用户配置了7列
     * 期望：程序异常退出，且错误消息符合预期
     * @throws Exception
     */
    @Test
    public void testRecordCountMoreThanConfig() throws Exception {
        List<Record> contents = new ArrayList<Record>();
        
        for (int i = 0; i < 10; i++)
        {
            Record r = new DefaultRecord();
            // pk
            r.addColumn(new StringColumn(String.valueOf(i)));
            r.addColumn(new LongColumn(i));
            r.addColumn(new LongColumn(i));
            r.addColumn(new LongColumn(i));
            
            // attr
            r.addColumn(new StringColumn(String.valueOf(i)));
            r.addColumn(new LongColumn(i));
            
            contents.add(r);
        }

        RecordReceiverForTest recordReceiver = new RecordReceiverForTest(contents);

        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.PUT_ROW);
        Configuration configuration = Configuration.newDefault();
        configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(conf));
        
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);

        OtsWriterSlaveProxy slave = new OtsWriterSlaveProxy();
        slave.init(configuration);
        try {
            slave.write(recordReceiver, collector);
            assertTrue(false);
        } catch (Exception e) {
            assertEquals(
                    "Size of record not equal size of config column. record size : 6, config column size : 7.",
                    e.getMessage());
        } finally {
            slave.close();
        }
    }
    

    /**
     * 输入：构造每个Record中有10列数据，用户配置了7列
     * 期望：程序异常退出，且错误消息符合预期
     * @throws Exception
     */
    @Test
    public void testRecordCountLessThanConfig() throws Exception {
        List<Record> contents = new ArrayList<Record>();
        
        for (int i = 0; i < 10; i++)
        {
            Record r = new DefaultRecord();
            // pk
            r.addColumn(new StringColumn(String.valueOf(i)));
            r.addColumn(new LongColumn(i));
            r.addColumn(new LongColumn(i));
            r.addColumn(new LongColumn(i));
            
            // attr
            r.addColumn(new StringColumn(String.valueOf(i)));
            r.addColumn(new LongColumn(i));
            r.addColumn(new DoubleColumn(i));
            r.addColumn(new BoolColumn(true));
            r.addColumn(new BytesColumn("s".getBytes()));
            r.addColumn(new LongColumn(i));
            
            contents.add(r);
        }

        RecordReceiverForTest recordReceiver = new RecordReceiverForTest(contents);
        
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.PUT_ROW);
        Configuration configuration = Configuration.newDefault();
        configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(conf));
        
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);

        OtsWriterSlaveProxy slave = new OtsWriterSlaveProxy();
        slave.init(configuration);
        try {
            slave.write(recordReceiver, collector);
            assertTrue(false);
        } catch (Exception e) {
            assertEquals(
                    "Size of record not equal size of config column. record size : 10, config column size : 7.",
                    e.getMessage());
        } finally {
            slave.close();
        }
    }
}
