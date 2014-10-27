package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.common.BaseTest;
import com.alibaba.datax.plugin.reader.otsreader.common.ReaderConf;
import com.alibaba.datax.plugin.reader.otsreader.common.SomketestTemplate;
import com.alibaba.datax.plugin.reader.otsreader.common.Utils;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

/**
 * 测试系统自动拆分范围的正确性
 * @author wanhong.chenwh@alibaba-inc.com
 *
 */

@RunWith(LoggedRunner.class)
public class SystemAutoSplitRangeFunctiontest extends SomketestTemplate{

    private static BaseTest base = new BaseTest("ots_reader_system_auto_split");

    @AfterClass
    public static void close() {
        base.close();
    }
    
    public PrimaryKeyType [][] getPKTypes() {
        PrimaryKeyType [][] types = {
                {PrimaryKeyType.INTEGER},
                {PrimaryKeyType.STRING},
                
                {PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                {PrimaryKeyType.INTEGER, PrimaryKeyType.STRING},
                {PrimaryKeyType.STRING, PrimaryKeyType.INTEGER},
                {PrimaryKeyType.STRING, PrimaryKeyType.STRING},
                
                {PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                {PrimaryKeyType.STRING, PrimaryKeyType.STRING, PrimaryKeyType.STRING, PrimaryKeyType.STRING},
                {PrimaryKeyType.STRING, PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                {PrimaryKeyType.INTEGER, PrimaryKeyType.STRING, PrimaryKeyType.STRING, PrimaryKeyType.STRING}
        };
        return types;
    }
    
    public void test(List<PrimaryKeyType> pk) {
        int lineCount = 500 + (new Random()).nextInt(500);
        int splitCount = 1 + (new Random()).nextInt(10);
        
        base.prepareData(pk, -100, lineCount, 0.5);
        
        ReaderConf readerConf = new ReaderConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        for (int i = 0; i < pk.size(); i++) {
            rangeBegin.add(PrimaryKeyValue.INF_MIN);
            rangeEnd.add(PrimaryKeyValue.INF_MAX);
        }
        
        readerConf.setConf(base.getConf(pk.size(), 10, 10, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, splitCount, noteRecordForTest);
        assertEquals(lineCount, noteRecordForTest.size());
        assertEquals(splitCount, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    public void test(PrimaryKeyType [][] types) {
        for (PrimaryKeyType [] pkTypes: types) {
            test(Arrays.asList(pkTypes));
        }
    }
    
    /**
     * 测试不同PK组合数据，测试Reader的正确性
     * 输入：混合的PK列，详见：getPKTypes()
     * 期望：期望程序能正常处理每种组合，且数据正确
     */
    @Test
    public void testMultiPKColumn() {
        test(getPKTypes());
    }
    
    /**
     * 特殊字符串的切分，如：\\:中国@#￥……^&*（））“”''`《》？：”|\\/
     * 输入：
     *      begin = ["", INF_MIN, INF_MIN, INF_MIN], 
     *      end = ["\\:中国@#￥……^&*（））“”''`《》？：”|\\/", INF_MAX, INF_MAX, INF_MAX]
     * 期望：系统能正常切分特殊字符串，且数据正确
     */
    @Test
    public void testSpecialString() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        
        base.prepareData(pk, 100, 123, 0.5);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromString(""));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromString("\\:中国@#￥……^&*（））“”''`《》？：”|\\/"));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);

        readerConf.setConf(base.getConf(pk.size(), 12, 4, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 19, noteRecordForTest);
        assertEquals(123, noteRecordForTest.size());
        assertEquals(19, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 特殊数值型的切分，如：Long.MIN_VALUE,Long.MAX_VALUE
     * 输入：
     *      begin = [Long.MIN_VALUE, INF_MIN, INF_MIN, INF_MIN]
     *      end = [Long.MAX_VALUE, INF_MAX, INF_MAX, INF_MAX]
     * 期望：系统能正常切分边界值，且数据正确
     */
    @Test
    public void testSpecialInteger() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, -12, 423, 0.5);
        
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

        readerConf.setConf(base.getConf(pk.size(), 2, 6, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 43, noteRecordForTest);
        assertEquals(423, noteRecordForTest.size());
        assertEquals(43, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试指定范围不够切分的情况
     * 输入：
     *      begin = [0, INF_MIN, INF_MIN, INF_MIN]
     *      end = [200, INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 300
     * 期望：能够拆分出156份，且数据正确
     */
    @Test
    public void testIntegerSplitNoEnoughRange() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 44, 199, 0.7);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(0));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(200));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);

        readerConf.setConf(base.getConf(pk.size(), 5, 5, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 300, noteRecordForTest);
        assertEquals(157, noteRecordForTest.size());
        assertEquals(156, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    // Integer相关的逻辑
    
    /**
     * 测试PartitionKey为Integer时，Begin和End是空数值的情况
     * 输入：
     *      Begin = []
     *      End = []
     *      adviceNum = 9
     * 期望：期望能切分为9份，且数据正确
     */
    @Test
    public void testIntegerEmptyBeginAndEnd() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 32, 432, 0.7);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        readerConf.setConf(base.getConf(pk.size(), 12, 2, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 9, noteRecordForTest);
        assertEquals(432, noteRecordForTest.size());
        assertEquals(9, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试PartitionKey为Integer时，Begin和End为[-1, INF_MAX)
     * 输入：
     *      Begin = [-1, INF_MIN, INF_MIN, INF_MIN]
     *      End = [INF_MAX, INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 132
     * 期望：期望能切分为100份，且数据正确
     */
    @Test
    public void testIntegerBeginAndINFMAXEnd() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, -1, 101, 0.3);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(-1));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        readerConf.setConf(base.getConf(pk.size(), 11, 0, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 132, noteRecordForTest);
        assertEquals(101, noteRecordForTest.size());
        assertEquals(100, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试PartitionKey为Integer时，Begin和End为[130, INF_MIN)
     * 输入：
     *      Begin = [-1, INF_MIN, INF_MIN, INF_MIN]
     *      End = [INF_MIN, INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 132
     * 期望：期望能切分为100份，且数据正确
     */
    @Test
    public void testIntegerBeginAndINFMINEnd() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, -1, 101, 0.3);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(130));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        readerConf.setConf(base.getConf(pk.size(), 11, 0, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 132, noteRecordForTest);
        assertEquals(101, noteRecordForTest.size());
        assertEquals(100, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试PartitionKey为Integer时，Begin和End为[INF_MAX, -1)
     * 输入：
     *      Begin = [INF_MAX, INF_MIN, INF_MIN, INF_MIN]
     *      End = [-1, INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 12
     * 期望：期望能切分为12份，且数据正确
     */
    @Test
    public void testIntegerINFMAXBeginAndEnd() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, -190, 999, 0.3);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(-1));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        readerConf.setConf(base.getConf(pk.size(), 5, 1, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 12, noteRecordForTest);
        assertEquals(809, noteRecordForTest.size());
        assertEquals(12, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试PartitionKey为Integer时，Begin和End为[INF_MIN, -1)
     * 输入：
     *      Begin = [INF_MIN, INF_MIN, INF_MIN, INF_MIN]
     *      End = [-1, INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 12
     * 期望：期望能切分为12份，且数据正确
     */
    @Test
    public void testIntegerINFMINBeginAndEnd() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, -190, 999, 0.3);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(-1));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        readerConf.setConf(base.getConf(pk.size(), 5, 1, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 12, noteRecordForTest);
        assertEquals(190, noteRecordForTest.size());
        assertEquals(12, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试PartitionKey为Integer时，Begin和End为[-1, 99)
     * 输入：
     *      Begin = [-1, INF_MIN, INF_MIN, INF_MIN]
     *      End = [99, INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 2
     * 期望：期望能切分为2份，且数据正确
     */
    @Test
    public void testIntegerBeginAndEnd() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 0, 200, 0.3);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(-1));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(99));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        readerConf.setConf(base.getConf(pk.size(), 2, 1, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 2, noteRecordForTest);
        assertEquals(100, noteRecordForTest.size());
        assertEquals(2, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    // String相关的逻辑
    /**
     * 测试PartitionKey为String时，Begin和End是空数值的情况
     * 输入：
     *      Begin = []
     *      End = []
     *      adviceNum = 3
     * 期望：期望能切分为3份，且数据正确
     */
    @Test
    public void testStringEmptyBeginAndEnd() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 12, 666, 0.7);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        readerConf.setConf(base.getConf(pk.size(), 3, 2, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 3, noteRecordForTest);
        assertEquals(666, noteRecordForTest.size());
        assertEquals(3, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试PartitionKey为String时，Begin和End为["", INF_MAX)
     * 输入：
     *      Begin = ["", INF_MIN, INF_MIN, INF_MIN]
     *      End = [INF_MAX, INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 11
     * 期望：期望能切分为11份，且数据正确
     */
    @Test
    public void testStringBeginAndINFMAXEnd() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, -90, 437, 0.7);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromString(""));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        readerConf.setConf(base.getConf(pk.size(), 13, 5, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 11, noteRecordForTest);
        assertEquals(437, noteRecordForTest.size());
        assertEquals(11, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试PartitionKey为String时，Begin和End为["500", INF_MIN)
     * 输入：
     *      Begin = ["500", INF_MIN, INF_MIN, INF_MIN]
     *      End = [INF_MIN, INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 11
     * 期望：期望能切分为11份，且数据正确
     */
    @Test
    public void testStringBeginAndINFMINEnd() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, -10, 100, 0.7);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromString("500"));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        readerConf.setConf(base.getConf(pk.size(), 13, 5, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 11, noteRecordForTest);
        assertEquals(57, noteRecordForTest.size());
        assertEquals(10, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试PartitionKey为String时，Begin和End为[INF_MAX, "")
     * 输入：
     *      Begin = [INF_MAX, INF_MIN, INF_MIN, INF_MIN]
     *      End = ["", INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 21
     * 期望：期望能切分为21份，且数据正确
     */
    @Test
    public void testStringINFMAXBeginAndEnd() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 0, 908, 0.7);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromString(""));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        readerConf.setConf(base.getConf(pk.size(), 0, 5, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 21, noteRecordForTest);
        assertEquals(908, noteRecordForTest.size());
        assertEquals(21, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试PartitionKey为String时，Begin和End为[INF_MIN, "99")
     * 输入：
     *      Begin = [INF_MIN, INF_MIN, INF_MIN, INF_MIN]
     *      End = ["99", INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 21
     * 期望：期望能切分为21份，且数据正确
     */
    @Test
    public void testStringINFMINBeginAndEnd() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 0, 908, 0.7);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromString("99"));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        readerConf.setConf(base.getConf(pk.size(), 0, 5, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 21, noteRecordForTest);
        assertEquals(908, noteRecordForTest.size());
        assertEquals(21, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试PartitionKey为String时，Begin和End为["", "中国")
     * 输入：
     *      Begin = ["", INF_MIN, INF_MIN, INF_MIN]
     *      End = ["中国", INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 21
     * 期望：期望能切分为21份，且数据正确
     */
    @Test
    public void testStringBeginAndEnd() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 111, 91, 1);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromString(""));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromString("中国"));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        readerConf.setConf(base.getConf(pk.size(), 0, 0, rangeBegin, rangeEnd, null));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 7, noteRecordForTest);
        assertEquals(91, noteRecordForTest.size());
        assertEquals(4, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
}
