package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
 * 测试用户指定范围的正确性
 * @author wanhong.chenwh@alibaba-inc.com
 *
 */

@RunWith(LoggedRunner.class)
public class UserSpecialPointSplitRangeFunctiontest extends SomketestTemplate{

    private static BaseTest base = new BaseTest("ots_reader_user_split_points_split");
    
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
    
    public PrimaryKeyValue[][] getPKValue() {
        List<PrimaryKeyValue> stringValues = new ArrayList<PrimaryKeyValue>();
        stringValues.add(PrimaryKeyValue.fromString("?"));
        stringValues.add(PrimaryKeyValue.fromString("A"));
        stringValues.add(PrimaryKeyValue.fromString("C"));
        stringValues.add(PrimaryKeyValue.fromString("Z"));
        stringValues.add(PrimaryKeyValue.fromString("a"));
        stringValues.add(PrimaryKeyValue.fromString("算"));
        
        List<PrimaryKeyValue> intValues = new ArrayList<PrimaryKeyValue>();
        intValues.add(PrimaryKeyValue.fromLong(Long.MIN_VALUE));
        intValues.add(PrimaryKeyValue.fromLong(-999999));
        intValues.add(PrimaryKeyValue.fromLong(-66666));
        intValues.add(PrimaryKeyValue.fromLong(0));
        intValues.add(PrimaryKeyValue.fromLong(11123223));
        intValues.add(PrimaryKeyValue.fromLong(Long.MAX_VALUE));
        
        PrimaryKeyValue[] strArray = new PrimaryKeyValue[stringValues.size()];
        PrimaryKeyValue[] intArray = new PrimaryKeyValue[intValues.size()];
        
        PrimaryKeyValue [][] values = {
                (PrimaryKeyValue[]) stringValues.toArray(strArray),
                (PrimaryKeyValue[]) intValues.toArray(intArray)
        };
        return values;
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
        
        if (pk.get(0) == PrimaryKeyType.STRING) {
            readerConf.setConf(base.getConf(pk.size(), 10, 10, rangeBegin, rangeEnd, Arrays.asList(this.getPKValue()[0])));
        } else {
            readerConf.setConf(base.getConf(pk.size(), 10, 10, rangeBegin, rangeEnd, Arrays.asList(this.getPKValue()[1])));
        }
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, splitCount, noteRecordForTest);
        assertEquals(lineCount, noteRecordForTest.size());
        assertEquals(7, subjobs.size());
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
    
    // Integer相关的测试
    
    /**
     * 测试split为空数据的情况Begin和End是空
     * 输入：
     *      begin = [-100, INF_MIN, INF_MIN, INF_MIN]
     *      end = [999, INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 19
     *      split = []
     * 期望：期望能切分为1份，且数据正确
     */
    @Test
    public void testIntegerEmptyPoints() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        
        base.prepareData(pk, 21, 123, 0.5);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.fromLong(-100));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.fromLong(999));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);

        readerConf.setConf(base.getConf(pk.size(), 6, 4, rangeBegin, rangeEnd, Collections.<PrimaryKeyValue> emptyList()));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 19, noteRecordForTest);
        assertEquals(123, noteRecordForTest.size());
        assertEquals(1, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试split有数据的时候，数值类型，
     * 输入：
     *      begin = [INF_MIN, INF_MIN, INF_MIN, INF_MIN]
     *      end = [INF_MAX, INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 3
     *      split = [Long.MIN_VALUE, -1121232, 0, 1328978, Long.MAX_VALUE]
     * 期望：期望能切分为6份，且数据正确
     */
    @Test
    public void testIntegerPoints() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        
        base.prepareData(pk, 198, 156, 0.5);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeSplit = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        rangeSplit.add(PrimaryKeyValue.fromLong(Long.MIN_VALUE));
        rangeSplit.add(PrimaryKeyValue.fromLong(-1121232));
        rangeSplit.add(PrimaryKeyValue.fromLong(0));
        rangeSplit.add(PrimaryKeyValue.fromLong(1328978));
        rangeSplit.add(PrimaryKeyValue.fromLong(Long.MAX_VALUE));
        
        readerConf.setConf(base.getConf(pk.size(), 12, 1, rangeBegin, rangeEnd, rangeSplit));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 3, noteRecordForTest);
        assertEquals(156, noteRecordForTest.size());
        assertEquals(6, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    // String相关的测试
    
    /**
     * 测试split为空数据的情况Begin和End是空
     * 输入：
     *      begin = ["", INF_MIN, INF_MIN, INF_MIN]
     *      end = ["\\:中国@#￥……^&*（））“”''`《》？：”|\\/", INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 19
     *      split = []
     * 期望：期望能切分为1份，且数据正确
     */
    @Test
    public void testStringEmptyPoints() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.INTEGER);
        
        base.prepareData(pk, 21, 123, 0.5);
        
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

        readerConf.setConf(base.getConf(pk.size(), 12, 4, rangeBegin, rangeEnd, Collections.<PrimaryKeyValue> emptyList()));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 19, noteRecordForTest);
        assertEquals(123, noteRecordForTest.size());
        assertEquals(1, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
    
    /**
     * 测试split有数据的时候，字符串型，且最后一个字符串长度很长，如10KB
     * 输入：
     *      begin = [INF_MIN, INF_MIN, INF_MIN, INF_MIN]
     *      end = [INF_MAX, INF_MAX, INF_MAX, INF_MAX]
     *      adviceNum = 99
     *      split:["", "0", "A", "S中国@#￥……^&*（））“”''`《》？：”|\\/S", "杭州...."]
     * 期望：期望能切分为6份，且数据正确
     */
    @Test
    public void testStringPoints() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, 0, 94, 0.5);
        
        ReaderConf readerConf = new ReaderConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        List<PrimaryKeyValue> rangeSplit = new ArrayList<PrimaryKeyValue>();
        
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 2000; i++) {
            sb.append("杭州");
        }
        
        rangeSplit.add(PrimaryKeyValue.fromString(""));
        rangeSplit.add(PrimaryKeyValue.fromString("0"));
        rangeSplit.add(PrimaryKeyValue.fromString("A"));
        rangeSplit.add(PrimaryKeyValue.fromString("S中国@#￥……^&*（））“”''`《》？：”|\\/"));
        rangeSplit.add(PrimaryKeyValue.fromString(sb.toString()));
        
        readerConf.setConf(base.getConf(pk.size(), 10, 1, rangeBegin, rangeEnd, rangeSplit));
        
        Configuration p = Configuration.from(Utils.getJsonConf(readerConf));
        List<Record> noteRecordForTest = new ArrayList<Record>();
        List<Configuration> subjobs = super.doReaderTest(p, 99, noteRecordForTest);
        assertEquals(94, noteRecordForTest.size());
        assertEquals(6, subjobs.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), readerConf.getConf(), noteRecordForTest));
    }
}
