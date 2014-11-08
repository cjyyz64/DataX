package com.alibaba.datax.common.util;

import com.alibaba.datax.common.exception.DataXException;
import org.hamcrest.core.StringContains;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.Callable;

public class RetryUtilTest {

    private static String OK = "I am ok now.";

    private static String BAD = "I am bad now.";

    /**
     * 模拟一个不靠谱的方法，其不靠谱体现在：调用它，前2次必定失败，第3次才能成功. 运行成功时，输出为：I am ok now.
     * 运行报错时，报错中信息为：I am bad now.
     */
    static class SomeService implements Callable<String> {
        private int i = 0;

        @Override
        public String call() throws Exception {
            i++;
            if (i <= 2) {
                throw new Exception(BAD);
            }
            return OK;
        }
    }

    @Test(timeout = 3000L)
    public void test1() throws Exception {
        long startTime = System.currentTimeMillis();

        String result = RetryUtil.executeWithRetry(new SomeService(), 3, 1000L,
                false);
        long endTime = System.currentTimeMillis();
        Assert.assertEquals(result, OK);
        long executeTime = endTime - startTime;

        System.out.println("executeTime:" + executeTime);
        Assert.assertTrue(executeTime < 3 * 1000L);
    }

    @Test(timeout = 3000L)
    public void test2() throws Exception {
        long startTime = System.currentTimeMillis();
        String result = RetryUtil.executeWithRetry(new SomeService(), 4, 1000L,
                false);
        long endTime = System.currentTimeMillis();
        Assert.assertEquals(result, OK);
        long executeTime = endTime - startTime;

        System.out.println("executeTime:" + executeTime);
        Assert.assertTrue(executeTime < 3 * 1000L);
    }

    @Test(timeout = 3000L)
    public void test3() throws Exception {
        long startTime = System.currentTimeMillis();
        String result = RetryUtil.executeWithRetry(new SomeService(), 40,
                1000L, false);
        long endTime = System.currentTimeMillis();
        Assert.assertEquals(result, OK);
        long executeTime = endTime - startTime;

        System.out.println("executeTime:" + executeTime);
        Assert.assertTrue(executeTime < 3 * 1000L);
    }

    @Test(timeout = 4000L)
    public void test4() throws Exception {
        long startTime = System.currentTimeMillis();
        String result = RetryUtil.executeWithRetry(new SomeService(), 40,
                1000L, true);
        long endTime = System.currentTimeMillis();
        Assert.assertEquals(result, OK);
        long executeTime = endTime - startTime;

        System.out.println("executeTime:" + executeTime);
        Assert.assertTrue(executeTime < 4 * 1000L);
        Assert.assertTrue(executeTime > 3 * 1000L);
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test(timeout = 3000L)
    public void test5() throws Exception {
        expectedEx.expect(Exception.class);
        expectedEx.expectMessage(StringContains.containsString(BAD));

        RetryUtil.executeWithRetry(new SomeService(), 2, 100L, false);
    }
}
