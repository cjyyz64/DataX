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
import com.alibaba.datax.plugin.reader.otsreader.common.Table;
import com.alibaba.datax.plugin.reader.otsreader.common.Utils;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.test.simulator.util.RecordSenderForTest;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

/**
 * 简单调用Reader
 * @author redchen
 *
 */
public class ReaderSample {
    
    private static String tableName = "reader_smaple";
    private static Configuration p = Utils.loadConf();
    private static OTSClient ots = new OTSClient(
            p.getString("endpoint"), 
            p.getString("accessid"), 
            p.getString("accesskey"), 
            p.getString("instance-name"));
    
    private static ReaderConf readerConf = null;
    
    private static void insertData() {
        List<PrimaryKeyType> pkType = new ArrayList<PrimaryKeyType>();
        pkType.add(PrimaryKeyType.STRING);
        pkType.add(PrimaryKeyType.STRING);
        pkType.add(PrimaryKeyType.STRING);
        pkType.add(PrimaryKeyType.STRING);
        List<ColumnType> attriTypes = new ArrayList<ColumnType>();
        attriTypes.add(ColumnType.STRING);
        attriTypes.add(ColumnType.STRING);
        attriTypes.add(ColumnType.STRING);
        attriTypes.add(ColumnType.STRING);
        Table t = new Table(ots, tableName, pkType, attriTypes, 0.5);
        t.create();
        t.insertData(200);
        t.close();
    }
    
    public static void prepareData() {
        insertData();
        
        readerConf = new ReaderConf();
        
        OTSConf conf = new OTSConf();
        conf.setEndpoint(p.getString("endpoint"));
        conf.setAccessId(p.getString("accessid"));
        conf.setAccesskey(p.getString("accesskey"));
        conf.setInstanceName(p.getString("instance-name"));
        conf.setTableName(tableName);
        
        List<OTSColumn> columns = new ArrayList<OTSColumn>();
        columns.add(OTSColumn.fromNormalColumn("pk_0"));
        columns.add(OTSColumn.fromNormalColumn("pk_1"));
        columns.add(OTSColumn.fromNormalColumn("pk_2"));
        columns.add(OTSColumn.fromNormalColumn("pk_3"));

        conf.setColumns(columns);
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromString("0"));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromString("中"));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeEnd(rangeEnd);
        
        List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();
        
        conf.setRangeSplit(splits);

        readerConf.setConf(conf);
        
    }
    
    public static void test() throws Exception {
        prepareData();
        Configuration p = Configuration.from(readerConf.toString());
        List<Record> noteRecordForTest = new LinkedList<Record>();
        PrintWriter printWriter  = new PrintWriter(System.out, true);
        RecordSender sender = new RecordSenderForTest(printWriter, noteRecordForTest);
        
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();
        OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();

        master.init(p);
        master.close();

        List<Configuration> cs = master.split(10);

        for (Configuration c : cs) {
            slave.read(sender, c);
        }
        System.out.println(noteRecordForTest.size());
    }

    public static void main(String[] args) throws Exception {
        ReaderSample.test();
    }
}
