package com.alibaba.datax.plugin.reader.otsreader.sample;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderMasterProxy;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderSlaveProxy;
import com.alibaba.datax.plugin.reader.otsreader.common.Person;
import com.alibaba.datax.plugin.reader.otsreader.common.ReaderConf;
import com.alibaba.datax.plugin.reader.otsreader.common.Table;
import com.alibaba.datax.plugin.reader.otsreader.common.Utils;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn.OTSColumnType;
import com.alibaba.datax.test.simulator.util.RecordSenderForTest;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

public class Sample {
    private String tableName = "ots_reader_sample";
    private ReaderConf readerConf = null;
    private Configuration p = Utils.loadConf();
    private OTSClient ots = new OTSClient(p.getString("endpoint"), p.getString("accessid"), p.getString("accesskey"), p.getString("instance-name"));
    
    private void prepare() throws FileNotFoundException {
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
        
        t.insertData(10000);
    }

    @Before
    public void setUp() throws Exception {
        prepare();
        
        Person person = new Person();
        person.setName("为硬音k，近似普通话轻声以外的g: cum,cīvis,facilis");
        person.setAge(Long.MAX_VALUE);
        person.setHeight(1111);
        person.setMale(false);
        
        readerConf = new ReaderConf();
        OTSConf conf = new OTSConf();
        conf.setEndpoint(p.getString("endpoint"));
        conf.setAccessId(p.getString("accessid"));
        conf.setAccesskey(p.getString("accesskey"));
        conf.setInstanceName(p.getString("instance-name"));
        conf.setTableName(tableName);

        conf.setRetry(10);
        conf.setSleepInMilliSecond(1000);
        
        List<OTSColumn> columns = new ArrayList<OTSColumn>();
        columns.add(new OTSColumn(ColumnValue.fromString("pk_0"), OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("pk_1"), OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("pk_2"), OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("pk_3"), OTSColumnType.NORMAL));
        
        columns.add(new OTSColumn(ColumnValue.fromString("attr_0"), OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("attr_1"), OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("attr_1"), OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("const_1"), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromLong(1342342L), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromDouble(-2299.0), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromBoolean(true), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromBoolean(false), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromBinary(Person.toByte(person)), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromString("attr_2"), OTSColumnType.NORMAL));
    
        conf.setColumns(columns);

        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeBegin(rangeBegin);
        
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeEnd(rangeEnd);
        
        readerConf.setConf(conf);
    }

    @Test
    public void testSampleBySpecify() throws Exception {
    
        Configuration p = Configuration.from(readerConf.toString());
        List<Record> noteRecordForTest = new LinkedList<Record>();
        PrintWriter printWriter  = new PrintWriter(System.out, true);
        RecordSender sender = new RecordSenderForTest(printWriter, noteRecordForTest);

        OtsReaderMasterProxy master = new OtsReaderMasterProxy();  
        OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();

        master.init(p);

        List<Configuration> cs = master.split(3);

        for (Configuration c : cs) {
            slave.read(sender, c);
        }
        assertEquals(10000, noteRecordForTest.size());
    }
    
    @Test
    public void testSampleByDefaultSplit() throws Exception {
        List<PrimaryKeyValue> points = new ArrayList<PrimaryKeyValue>();
        readerConf.getConf().setRangeSplit(points);
        Configuration p = Configuration.from(readerConf.toString());
        List<Record> noteRecordForTest = new LinkedList<Record>();
        PrintWriter printWriter  = new PrintWriter(System.out, true);
        RecordSender sender = new RecordSenderForTest(printWriter, noteRecordForTest);

        OtsReaderMasterProxy master = new OtsReaderMasterProxy();  
        OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();

        master.init(p);

        List<Configuration> cs = master.split(1);

        for (Configuration c : cs) {
            slave.read(sender, c);
        }
        assertEquals(10000, noteRecordForTest.size());
    }
}
