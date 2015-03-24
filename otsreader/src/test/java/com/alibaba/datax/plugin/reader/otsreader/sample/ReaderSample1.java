package com.alibaba.datax.plugin.reader.otsreader.sample;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderMasterProxy;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderSlaveProxy;
import com.alibaba.datax.plugin.reader.otsreader.common.ReaderConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.test.simulator.util.RecordSenderForTest;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

/**
 * 简单调用Reader
 * @author redchen
 *
 */
public class ReaderSample1 {
    
    private static String tableName = "ots_datax";
    
    private static ReaderConf readerConf = null;
    
    public static void prepareData() {

        readerConf = new ReaderConf();
        
        OTSConf conf = new OTSConf();
        conf.setEndpoint("http://10.101.200.36");
        conf.setAccessId("OTSMultiUser001_accessid");
        conf.setAccesskey("OTSMultiUser001_accesskey");
        conf.setInstanceName("TestInstance001");
        conf.setTableName(tableName);
        
        List<OTSColumn> columns = new ArrayList<OTSColumn>();
        columns.add(OTSColumn.fromNormalColumn("pk_0"));
        columns.add(OTSColumn.fromNormalColumn("pk_1"));
        columns.add(OTSColumn.fromNormalColumn("pk_2"));
        columns.add(OTSColumn.fromNormalColumn("pk_3"));

        conf.setColumns(columns);
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromString("[S"));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromString("\\:"));
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeEnd(rangeEnd);
        
        List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();
        
        conf.setRangeSplit(splits);

        readerConf.setConf(conf);
    }
    
    public static void test() throws Exception {
        prepareData();
        System.out.println(readerConf.toString());
        Configuration p = Configuration.from(readerConf.toString());
        List<Record> noteRecordForTest = new LinkedList<Record>();
        PrintWriter printWriter  = new PrintWriter(System.out, true);
        RecordSender sender = new RecordSenderForTest(printWriter, noteRecordForTest);
        
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();
        OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();

        master.init(p);
        master.close();

        List<Configuration> cs = master.split(1);

        for (Configuration c : cs) {
            slave.read(sender, c);
        }
        System.out.println(noteRecordForTest.size());
    }

    public static void main(String[] args) throws Exception {
        ReaderSample1.test();
    }
}
