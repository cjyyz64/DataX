package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.common.BaseTest;
import com.alibaba.datax.plugin.reader.otsreader.common.ReaderConf;
import com.alibaba.datax.plugin.reader.otsreader.common.SomketestTemplate;
import com.alibaba.datax.plugin.reader.otsreader.common.Utils;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

/**
 * 测试指定混合列时，导出数据正常
 * @author wanhong.chenwh@alibaba-inc.com
 *
 */

@RunWith(LoggedRunner.class)
public class ColumnMixFunctiontest extends SomketestTemplate{

    private static BaseTest base = new BaseTest("ots_reader_column_mix");

    @AfterClass
    public static void close() {
        base.close();
    }
    
    /**
     * 测试全PK的导出正确性
     * 输入：column = [pk_0~pk_3]
     * 期望：正常导出数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testAllPK() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 1233323, 111, 0.2);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(Long.MIN_VALUE));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(Long.MAX_VALUE));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);

        readerConf.setConf(base.getConf(pk.size(), 0, 0, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 110, noteRecordForTest);
        assertEquals(111, noteRecordForTest.size());
        assertEquals(110, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试全PK，且有重复的PK列的导出正确性
     * 输入：column = [pk_0~pk_3, pk_0, pk_3, pk_1, pk_0, pk_1, pk_0, pk_3, pk_3, pk_2]
     * 期望：正常导出数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testAllPKAndRepeat() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 7654, 111, 0.2);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(Long.MIN_VALUE));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(Long.MAX_VALUE));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);

        OTSConf conf = base.getConf(pk.size(), 0, 0, rangeBegin, rangeEnd, null);
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_3"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_1"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_1"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_3"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_3"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_2"));
        readerConf.setConf(conf);
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 9, noteRecordForTest);
        assertEquals(111, noteRecordForTest.size());
        assertEquals(9, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试全Attri的导出正确性
     * 输入：column = [attr_0~attr_16]
     * 期望：正常导出数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testAllAttri() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, -183, 419, 0);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(Long.MIN_VALUE));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(Long.MAX_VALUE));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);

        readerConf.setConf(base.getConf(0, 17, 0, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 2, noteRecordForTest);
        assertEquals(419, noteRecordForTest.size());
        assertEquals(2, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试全Attri,且有重复的Attr的导出正确性
     * 输入：column = [attr_0~attr_16, attr_2, attr_10, attr_0, attr_0, attr_5, attr_0, attr_0, attr_9, attr_0, attr_16]
     * 期望：正常导出数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testAllAttriAndRepeat() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 111, 419, 0);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(Long.MIN_VALUE));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(Long.MAX_VALUE));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        OTSConf conf = base.getConf(0, 17, 0, rangeBegin, rangeEnd, null);
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_2"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_10"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_5"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_9"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_16"));
        readerConf.setConf(conf);
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 2, noteRecordForTest);
        assertEquals(419, noteRecordForTest.size());
        assertEquals(2, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试PK和Attri混合的导出正确性
     * 输入：column = [pk_0~pk_3, attr_0~attr_16]
     * 期望：正常导出数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testPKAndAttri() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 109, 125, 0.2);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(Long.MIN_VALUE));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(Long.MAX_VALUE));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);

        readerConf.setConf(base.getConf(pk.size(), 17, 0, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 9, noteRecordForTest);
        assertEquals(125, noteRecordForTest.size());
        assertEquals(9, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试只有一个列名，但是重复出现
     * 输入：column = [attr_0(重复5次)]
     * 期望：正常导出数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void test1ColumnAndRepeat() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);

        base.prepareData(pk, Long.MIN_VALUE, 125, 0);

        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(Long.MIN_VALUE));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(Long.MAX_VALUE));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);

        OTSConf conf = base.getConf(0, 1, 0, rangeBegin, rangeEnd, null);
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_0"));
        readerConf.setConf(conf);
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();

        List<Configuration> subjobs = super.doReaderTest(p, 3, noteRecordForTest);

        assertEquals(125, noteRecordForTest.size());
        assertEquals(3, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试PK和Const混合的导出正确性
     * 输入：column = [pk_0~pk_3, const_0~const_9]
     * 期望：正常导出数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testPKAndConst() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, Integer.MIN_VALUE, 768, 0.2);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(Long.MIN_VALUE));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(Long.MAX_VALUE));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);

        readerConf.setConf(base.getConf(pk.size(), 0, 10, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 43, noteRecordForTest);
        assertEquals(768, noteRecordForTest.size());
        assertEquals(43, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试Attri和Const的导出正确性
     * 输入：column = [attr_0~attr_9, const_0~const_2]
     * 期望：正常导出数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testAttriAndConst() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 909, 2000, 0);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(Long.MIN_VALUE));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(Long.MAX_VALUE));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);

        readerConf.setConf(base.getConf(0, 10, 3, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 10, noteRecordForTest);
        assertEquals(2000, noteRecordForTest.size());
        assertEquals(10, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试PK、Attri和Const的导出正确性，且PK和Attri都有多个重复列的情况
     * 输入：column = [pk_0~pk_3, attr_0~attr_9, const_0, pk_3, pk_3, pk_0, pk_1, pk_10, attr_0, attr_0, attr_2, attr_1, attr_1, attr_7, attr_0, attr_4, attr_20]
     * 期望：正常导出数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testPKAndAttriAndConst() throws Exception {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 122, 10000, 0.2);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(Long.MIN_VALUE));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(Long.MAX_VALUE));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        OTSConf conf = base.getConf(pk.size(), 10, 1, rangeBegin, rangeEnd, null);
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_3"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_3"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_1"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("pk_10"));
        
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_2"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_1"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_1"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_7"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_0"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_4"));
        conf.getColumns().add(OTSColumn.fromNormalColumn("attr_20"));
        readerConf.setConf(conf);
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 3, noteRecordForTest);
        assertEquals(10000, noteRecordForTest.size());
        assertEquals(3, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
}
