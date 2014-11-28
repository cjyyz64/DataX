package com.alibaba.datax.plugin.writer.otswriter.sample;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.otswriter.OtsWriterSlaveProxy;
import com.alibaba.datax.plugin.writer.otswriter.common.BaseTest;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector.RecordAndMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSPKColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf.RestrictConf;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.alibaba.datax.test.simulator.util.RecordReceiverForTest;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.PrimaryKeyType;

public class TestSlave {
    
    public static String tableName = "ots_writer_test_writer";
    public static BaseTest base = new BaseTest(tableName);
    
    public static void prepare() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        
        base.prepareData(pk, 0, 0, 0);
    }
    
    public static Configuration getConf() {
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
        conf.setPrimaryKeyColumn(primaryKeyColumn);
        
        List<OTSAttrColumn> attributeColumn = new ArrayList<OTSAttrColumn>();
        attributeColumn.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attributeColumn.add(new OTSAttrColumn("attr_0", ColumnType.INTEGER));
        attributeColumn.add(new OTSAttrColumn("attr_2", ColumnType.DOUBLE));
        attributeColumn.add(new OTSAttrColumn("attr_3", ColumnType.DOUBLE));
        attributeColumn.add(new OTSAttrColumn("attr_4", ColumnType.BINARY));
        
        conf.setAttributeColumn(attributeColumn);
        
        conf.setOperation(OTSOpType.UPDATE_ROW);
        
        conf.setRetry(1);
        conf.setSleepInMilliSecond(100);
        conf.setBatchWriteCount(50);
        conf.setConcurrencyWrite(5);
        conf.setIoThreadCount(1);
        
        RestrictConf restrictConf = conf.new RestrictConf();
        restrictConf.setRequestTotalSizeLimition(1024*1024);
        conf.setRestrictConf(restrictConf);
        
        configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(conf));
        
        return configuration;
    }

    /**
     * 1.是否允许Attribute为空？
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        List<Record> contents = new ArrayList<Record>();
        
//        for (int i = 0; i < 10; i++)
//        {
//            Record r = new DefaultRecord();
//            // pk
//            r.addColumn(new StringColumn(String.valueOf(i)));
//            r.addColumn(new LongColumn(i));
//            
//            // attr
//            r.addColumn(new StringColumn(String.valueOf(i)));
//            r.addColumn(new LongColumn(i));
//            r.addColumn(new DoubleColumn(i));
//            r.addColumn(new BoolColumn(true));
//            r.addColumn(new BytesColumn("hello".getBytes()));
//            
//            contents.add(r);
//        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1024; i++) {
            sb.append("a");
        }
        for (int i = 0; i < 11; i++)
        {
            Record r = new DefaultRecord();
            // pk
            r.addColumn(new StringColumn(""));
            r.addColumn(new LongColumn(i));
            
            // attr
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new LongColumn(i));
            r.addColumn(new DoubleColumn(i));
            r.addColumn(new BoolColumn(true));
            r.addColumn(new BytesColumn(sb.toString().getBytes()));
            
            contents.add(r);
        }

        RecordReceiverForTest recordReceiver = new RecordReceiverForTest(contents);

        TestSlave.prepare();
        TestSlave.base.getOts().shutdown();
        
        Configuration configuration = TestSlave.getConf();
        
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);

        OtsWriterSlaveProxy slave = new OtsWriterSlaveProxy();
        slave.init(configuration);
        try {
            slave.write(recordReceiver, collector);
        } finally {
            slave.close();
        }
        System.out.println("================================ dirty collector =======================");
        System.out.println("Size : " + collector.getContent().size());
        for (RecordAndMessage rm : collector.getContent()) {
            System.out.println(rm.getErrorMessage());
        }
    }

}
