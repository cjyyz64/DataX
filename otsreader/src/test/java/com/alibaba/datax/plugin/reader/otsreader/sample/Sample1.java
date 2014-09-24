package com.alibaba.datax.plugin.reader.otsreader.sample;

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

public class Sample1 {
    private String tableName = "ots_reader_smaple_1";
    private ReaderConf readerConf = null;
    private Configuration p = Utils.loadConf();
    private OTSClient ots = new OTSClient(
            p.getString("endpoint"), 
            p.getString("accessid"), 
            p.getString("accesskey"), 
            p.getString("instance-name"));
    
    private Person getPerson() {
        Person person = new Person();
        person.setName("][]]fs====34534");
        person.setAge(Long.MIN_VALUE);
        person.setHeight(1111);
        person.setMale(false);
        return person;
    }

    @Before
    public void setUp() throws Exception {
        // prepare table
        List<PrimaryKeyType> pkType = new ArrayList<PrimaryKeyType>();
        pkType.add(PrimaryKeyType.STRING);
        pkType.add(PrimaryKeyType.INTEGER);
        pkType.add(PrimaryKeyType.INTEGER);
        pkType.add(PrimaryKeyType.STRING);

        List<ColumnType> attriTypes = new ArrayList<ColumnType>();
        attriTypes.add(ColumnType.STRING);
        attriTypes.add(ColumnType.INTEGER);
        attriTypes.add(ColumnType.DOUBLE);
        attriTypes.add(ColumnType.BOOLEAN);
        attriTypes.add(ColumnType.BINARY);

        Table t = new Table(ots, tableName, pkType, attriTypes, 0.5);
        t.create();

        t.insertData(10000);

        // build conf
        readerConf = new ReaderConf();
        OTSConf conf = new OTSConf();
        conf.setEndpoint(p.getString("endpoint"));
        conf.setAccessId(p.getString("accessid"));
        conf.setAccesskey(p.getString("accesskey"));
        conf.setInstanceName(p.getString("instance-name"));
        conf.setTableName(tableName);

        conf.setRetry(1);
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
        columns.add(new OTSColumn(ColumnValue.fromBinary(Person.toByte(getPerson())), OTSColumnType.CONST));

        columns.add(new OTSColumn(ColumnValue.fromString("attr_2"), OTSColumnType.NORMAL));

        conf.setColumns(columns);

        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromString("5"));
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeEnd(rangeEnd);

        readerConf.setConf(conf);
    }

    @Test
    public void testCase() throws Exception {
        PrintWriter printWriter = new PrintWriter(System.out, true);
        List<Record> noteRecordForTest = new LinkedList<Record>();
        RecordSender sender = new RecordSenderForTest(printWriter, noteRecordForTest);
        
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();
        OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();
        Configuration p = Configuration.from(readerConf.toString());

        master.init(p);
        List<Configuration> cs = master.split(10);
        
        for (Configuration c : cs) {
            slave.read(sender, c);
        }
        System.out.println(noteRecordForTest.size());
    }
}
