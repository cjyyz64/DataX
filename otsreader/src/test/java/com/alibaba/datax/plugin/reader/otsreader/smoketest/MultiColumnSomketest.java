package com.alibaba.datax.plugin.reader.otsreader.smoketest;

import static org.junit.Assert.assertEquals;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
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
public class MultiColumnSomketest extends BasicReaderPluginTest{
    private String tableName = "ots_reader_somkettest_multicolumn";
    private ReaderConf readerConf = null;
    private Configuration p = Utils.loadConf();
    private OTSClient ots = new OTSClient(
            p.getString("endpoint"), 
            p.getString("accessid"), 
            p.getString("accesskey"), 
            p.getString("instance-name"));
    
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
    public void testCaseAllAttrColumn() throws Exception {
        prepare4PK(10000);
        
        {
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
            
            for (int i = 0; i < 50; i++) {
                columns.add(new OTSColumn(ColumnValue.fromString("attr_" + i), OTSColumnType.NORMAL));
            }
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

        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));

        List<Record> noteRecordForTest = new ArrayList<Record>();

        List<Configuration> subjobs = super.doReaderTest(p, 10, noteRecordForTest);

        assertEquals(10000, noteRecordForTest.size());
        assertEquals(10, subjobs.size());
        assertEquals(true, Utils.checkOutput(ots, readerConf.getConf(), noteRecordForTest));
    }
    
    @Test
    public void testCaseAllPK() throws Exception {
        prepare4PK(10000);
        
        {
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
            
            for (int i = 0; i < 5; i++) {
                for (int j  = 0; j < 10; j++) {
                    columns.add(new OTSColumn(ColumnValue.fromString("pk_" + j), OTSColumnType.NORMAL));
                }
                
            }
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

        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));

        List<Record> noteRecordForTest = new ArrayList<Record>();

        List<Configuration> subjobs = super.doReaderTest(p, 10, noteRecordForTest);

        assertEquals(10000, noteRecordForTest.size());
        assertEquals(10, subjobs.size());
        assertEquals(true, Utils.checkOutput(ots, readerConf.getConf(), noteRecordForTest));
    }
}
