package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Test;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderMasterProxy;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderSlaveProxy;
import com.alibaba.datax.plugin.reader.otsreader.common.BaseTest;
import com.alibaba.datax.plugin.reader.otsreader.common.ReaderConf;
import com.alibaba.datax.plugin.reader.otsreader.common.Utils;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.test.simulator.util.RecordSenderForTest;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;


/**
 * 测试用户请求columns个数的正确性
 * @author wanhong.chenwh@alibaba-inc.com
 *
 */
public class ColumnCountLimitFunctiontest {

    private static BaseTest base = new BaseTest("ots_reader_column_count_limit");

    @AfterClass
    public static void close() {
        base.close();
    }
    
    /**
     * 测试读取128列数据，正常导出数据
     * 输入：column = [pk0~pk3, attr0~attr123]
     * 期望：正常导出数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void test128Column() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        
        base.prepareData(pk, -100, 1000, 0.5);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        readerConf.setConf(base.getConf(pk.size(), 124, 0, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(readerConf.toString());
        
        List<Record> noteRecordForTest = new LinkedList<Record>();
        RecordSender sender = new RecordSenderForTest(null, noteRecordForTest);
        
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();
        OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();

        master.init(p);
        List<Configuration> cs = master.split(10);
        master.close();

        for (Configuration c : cs) {
            slave.read(sender, c);
        }

        assertEquals(1000, noteRecordForTest.size());
        assertEquals(10, cs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试读取129列数据
     * 输入：column = [pk0~pk3, attr0~attr124]
     * 期望：系统异常退出
     * @throws Exception 
     */
    @Test
    public void test129Column() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        
        base.prepareData(pk, 0, 99, 0.5);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        readerConf.setConf(base.getConf(pk.size(), 125, 0, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(readerConf.toString());
        
        List<Record> noteRecordForTest = new LinkedList<Record>();
        RecordSender sender = new RecordSenderForTest(null, noteRecordForTest);
        
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();
        OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();

        master.init(p);
        List<Configuration> cs = master.split(10);
        master.close();

        try {
            for (Configuration c : cs) {
                slave.read(sender, c);
            }
            assertTrue(false);
        } catch (OTSException e) {
            assertEquals("The number of columns from the request exceeded the limit.", e.getMessage());
        }
    }
    
    /**
     * 测试读129列（128列无重复+1列重复列）数据，
     * 输入：column = [pk0~pk3, attr0~attr123, attr_0]
     * 期望：系统异常退出
     * @throws Exception 
     */
    @Test
    public void test128ColumnWith1RepeatColumn() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        
        base.prepareData(pk, -1, 99, 0.5);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        OTSConf conf = base.getConf(pk.size(), 124, 0, rangeBegin, rangeEnd, null);
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_0"));
        
        readerConf.setConf(conf);
        
        Configuration p = Configuration.from(readerConf.toString());
        
        List<Record> noteRecordForTest = new LinkedList<Record>();
        RecordSender sender = new RecordSenderForTest(null, noteRecordForTest);
        
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();
        OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();

        master.init(p);
        List<Configuration> cs = master.split(10);
        master.close();

        try {
            for (Configuration c : cs) {
                slave.read(sender, c);
            }
            assertTrue(false);
        } catch (OTSException e) {
            assertEquals("The number of columns from the request exceeded the limit.", e.getMessage());
        }
    }
    
    /**
     * 测试读129列（128列无重复+1列常量列）数据
     * 输入：column = [pk0~pk3, attr0~attr123, const_0]
     * 期望：正常导出数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void test128ColumnWith1ConstColumn() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        
        base.prepareData(pk, -199, 99, 0.5);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        OTSConf conf = base.getConf(pk.size(), 124, 1, rangeBegin, rangeEnd, null);
        
        readerConf.setConf(conf);
        
        Configuration p = Configuration.from(readerConf.toString());
        
        List<Record> noteRecordForTest = new LinkedList<Record>();
        RecordSender sender = new RecordSenderForTest(null, noteRecordForTest);
        
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();
        OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();

        master.init(p);
        List<Configuration> cs = master.split(10);
        master.close();

        for (Configuration c : cs) {
            slave.read(sender, c);
        }

        assertEquals(99, noteRecordForTest.size());
        assertEquals(10, cs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试129列常量列
     * 输入：column = [pk_0~pk_3, const_0~const_128]
     * 期望：正常导出数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void test129ConstColumn() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        
        base.prepareData(pk, 122, 576, 0.5);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        readerConf.setConf(base.getConf(pk.size(), 0, 129, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(readerConf.toString());
        
        List<Record> noteRecordForTest = new LinkedList<Record>();
        RecordSender sender = new RecordSenderForTest(null, noteRecordForTest);
        
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();
        OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();

        master.init(p);
        List<Configuration> cs = master.split(4);
        master.close();

        for (Configuration c : cs) {
            slave.read(sender, c);
        }

        assertEquals(576, noteRecordForTest.size());
        assertEquals(4, cs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
}
