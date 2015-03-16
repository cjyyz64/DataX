package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.plugin.reader.odpsreader.util.OdpsSplitUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Method;
import java.util.List;

public class OpdsSplitUtilTest {
    private static Method splitRecordCountMethod;

    @BeforeClass
    public static void initMethod() throws Exception {
        Class testClass = Class.forName(OdpsSplitUtil.class.getName());

        splitRecordCountMethod = testClass.getDeclaredMethod("splitRecordCount", new Class[]{long.class, int.class});

        splitRecordCountMethod.setAccessible(true);
    }

    @AfterClass
    public static void destroyMethod() {
        splitRecordCountMethod = null;
    }


    @Test
    public void testSplitRecordCount_00() throws Exception {
        long recordCount = 100;
        int adviceNum = 3;

        List<Pair<Long, Long>> splitResult = (List<Pair<Long, Long>>) splitRecordCountMethod.invoke(null, new Object[]{recordCount, adviceNum});

        long sum = 0;
        for (Pair<Long, Long> longLongPair : splitResult) {
            sum += longLongPair.getRight().longValue();
        }

        Assert.assertTrue(sum == recordCount);
        Assert.assertTrue(splitResult.size() == adviceNum);
        System.out.println(splitResult);
    }

    @Test
    public void testSplitRecordCount_01() throws Exception {
        long recordCount = 8;
        int adviceNum = 3;

        List<Pair<Long, Long>> splitResult = (List<Pair<Long, Long>>) splitRecordCountMethod.invoke(null, new Object[]{recordCount, adviceNum});

        long sum = 0;
        for (Pair<Long, Long> longLongPair : splitResult) {
            sum += longLongPair.getRight().longValue();
        }

        Assert.assertTrue(sum == recordCount);
        Assert.assertTrue(splitResult.size() == adviceNum);
        System.out.println(splitResult);
    }

    @Test
    public void testSplitRecordCount_02() throws Exception {
        long recordCount = 1;
        int adviceNum = 4;

        List<Pair<Long, Long>> splitResult = (List<Pair<Long, Long>>) splitRecordCountMethod.invoke(null, new Object[]{recordCount, adviceNum});

        long sum = 0;
        for (Pair<Long, Long> longLongPair : splitResult) {
            sum += longLongPair.getRight().longValue();
        }

        Assert.assertTrue(sum == recordCount);
        Assert.assertTrue(splitResult.size() == 1);
        System.out.println(splitResult);
    }

    @Test
    public void testSplitRecordCount_03() throws Exception {
        long recordCount = 0;
        int adviceNum = 3;

        List<Pair<Long, Long>> splitResult = (List<Pair<Long, Long>>) splitRecordCountMethod.invoke(null, new Object[]{recordCount, adviceNum});

        long sum = 0;
        for (Pair<Long, Long> longLongPair : splitResult) {
            sum += longLongPair.getRight().longValue();
        }

        Assert.assertTrue(sum == recordCount);
        Assert.assertTrue(splitResult.size() == 1);
        System.out.println(splitResult);
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testSplitRecordCount_04() throws Exception {
        long recordCount = -1;
        int adviceNum = 3;

        expectedEx.expect(Exception.class);
        List<Pair<Long, Long>> splitResult = (List<Pair<Long, Long>>) splitRecordCountMethod.invoke(null, new Object[]{recordCount, adviceNum});
    }

}
