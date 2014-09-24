package com.alibaba.datax.plugin.reader.otsreader.smoketest;

import static org.junit.Assert.assertEquals;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.common.Person;
import com.alibaba.datax.plugin.reader.otsreader.common.ReaderConf;
import com.alibaba.datax.plugin.reader.otsreader.common.Table;
import com.alibaba.datax.plugin.reader.otsreader.common.Utils;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn.OTSColumnType;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.test.simulator.BasicReaderPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

@RunWith(LoggedRunner.class)
public class BaseSomketest extends BasicReaderPluginTest{
    private String tableName = "ots_reader_somkettest_base";
    private ReaderConf readerConf = null;
    private Configuration p = Utils.loadConf();
    private OTSClient ots = new OTSClient(
            p.getString("endpoint"), 
            p.getString("accessid"), 
            p.getString("accesskey"), 
            p.getString("instance-name"));

    private Person getPerson() {
        Person person = new Person();
        person.setName("为硬音k，近似普通话轻声以外的g: cum,cīvis,facilis");
        person.setAge(Long.MAX_VALUE);
        person.setHeight(1111);
        person.setMale(false);
        return person;
    }

    private void prepare1PK(int count)  throws Exception {
        // prepare table
        List<PrimaryKeyType> pkType = new ArrayList<PrimaryKeyType>();
        pkType.add(PrimaryKeyType.STRING);
        List<ColumnType> attriTypes = new ArrayList<ColumnType>();
        attriTypes.add(ColumnType.STRING);
        attriTypes.add(ColumnType.INTEGER);
        attriTypes.add(ColumnType.DOUBLE);
        attriTypes.add(ColumnType.BOOLEAN);
        attriTypes.add(ColumnType.BINARY);
        Table t = new Table(ots, tableName, pkType, attriTypes, 0.5);
        t.create();
        t.insertData(count);

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
        columns.add(new OTSColumn(ColumnValue.fromString("const_1"), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromLong(1342342L), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromDouble(-2299.0), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromBoolean(true), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromString("attr_0"), OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("attr_1"), OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("attr_1"), OTSColumnType.NORMAL));
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

    private void prepare2PK(int count)  throws Exception {
        // prepare table
        List<PrimaryKeyType> pkType = new ArrayList<PrimaryKeyType>();
        pkType.add(PrimaryKeyType.INTEGER);
        pkType.add(PrimaryKeyType.INTEGER);
        List<ColumnType> attriTypes = new ArrayList<ColumnType>();
        attriTypes.add(ColumnType.STRING);
        attriTypes.add(ColumnType.INTEGER);
        attriTypes.add(ColumnType.DOUBLE);
        attriTypes.add(ColumnType.BOOLEAN);
        attriTypes.add(ColumnType.BINARY);
        Table t = new Table(ots, tableName, pkType, attriTypes, 0.5);
        t.create();
        t.insertData(count);

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
        
        columns.add(new OTSColumn(ColumnValue.fromString("const_1"), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromLong(1342342L), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromDouble(-2299.0), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromBoolean(true), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromString("attr_0"), OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("attr_1"), OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("attr_1"), OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromBoolean(false), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromString("pk_2"), OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("pk_3"), OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromBinary(Person.toByte(getPerson())), OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromString("attr_2"), OTSColumnType.NORMAL));
        conf.setColumns(columns);

        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromLong(4000));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeEnd(rangeEnd);

        readerConf.setConf(conf);
    }

    private void prepare4PK(int count) throws Exception {
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
        t.insertData(count);

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

    @Override
    protected OutputStream buildDataOutput(String optionalOutputName) {
        return null;
        //return System.out;
    }

    @Override
    public String getTestPluginName() {
        return "otsreader";
    }

    @Test
    public void testCase1PK() throws Exception {
        prepare1PK(10000);

        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));

        List<Record> noteRecordForTest = new ArrayList<Record>();

        List<Configuration> subjobs = super.doReaderTest(p, 100, noteRecordForTest);

        assertEquals(5555, noteRecordForTest.size());
        assertEquals(100, subjobs.size());
        assertEquals(true, Utils.checkOutput(ots, readerConf.getConf(), noteRecordForTest));
    }

    @Test
    public void testCase2PK() throws Exception {
        prepare2PK(8888);

        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));

        List<Record> noteRecordForTest = new ArrayList<Record>();

        List<Configuration> subjobs = super.doReaderTest(p, 1000, noteRecordForTest);

        assertEquals(4000, noteRecordForTest.size());
        assertEquals(1000, subjobs.size());
        assertEquals(true, Utils.checkOutput(ots, readerConf.getConf(), noteRecordForTest));
    }

    @Test
    public void testCase4PK() throws Exception {
        prepare4PK(10000);

        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));

        List<Record> noteRecordForTest = new ArrayList<Record>();

        List<Configuration> subjobs = super.doReaderTest(p, 10, noteRecordForTest);

        assertEquals(10000, noteRecordForTest.size());
        assertEquals(10, subjobs.size());
        assertEquals(true, Utils.checkOutput(ots, readerConf.getConf(), noteRecordForTest));
    }
}
