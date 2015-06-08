package com.alibaba.datax.plugin.writer.temp_odpswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.odpswriter.OdpsWriterErrorCode;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsUtil;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;

/**
 * Created by hongjiao.hj on 2015/6/1.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({OdpsUtil.class})
public class OdpsUtilTest {

    @Test
    public void testRunSqlTaskWithRetry_重试3次_时间大于7s() {
        long startTime = System.currentTimeMillis();
        try {
            OdpsUtil.runSqlTaskWithRetry(
                    new Odps(new AliyunAccount("datax_test_ID", "datax_test_key")), "select * from table",
                    4, 1000, true);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof DataXException);
            Assert.assertTrue(OdpsWriterErrorCode.RUN_SQL_ODPS_EXCEPTION.equals(((DataXException) e).getErrorCode()));
            System.out.println("ok");
        }
        long endTime = System.currentTimeMillis();
        long sleepInterval = endTime - startTime;
        System.out.println("sleepInterval: " + sleepInterval);
        Assert.assertTrue(sleepInterval > 7000);
    }

    @Test
    public void testRunSqlTaskWithRetryOK() throws Exception {
        PowerMockito.spy(OdpsUtil.class);
        final AtomicInteger realRetryTimes = new AtomicInteger(0);
        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                realRetryTimes.addAndGet(1);
                System.out.println("执行成功，无须重试");
                return null;
            }
        }).when(OdpsUtil.class, "runSqlTask", (Odps) anyObject(), anyString());

        OdpsUtil.runSqlTaskWithRetry(
                new Odps(new AliyunAccount("datax_test_ID", "datax_test_key")), "select * from table",
                4, 1000, true);
        Assert.assertEquals(1, realRetryTimes.get());
    }

    @Test
    public void testRunSqlTaskWithRetry_不可重试Exception() throws Exception {
        PowerMockito.spy(OdpsUtil.class);
        final AtomicInteger realRetryTimes = new AtomicInteger(0);
        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                realRetryTimes.addAndGet(1);
                throw new Exception("不可重试异常");
            }
        }).when(OdpsUtil.class, "runSqlTask", (Odps) anyObject(), anyString());

        try {
            OdpsUtil.runSqlTaskWithRetry(
                    new Odps(new AliyunAccount("datax_test_ID", "datax_test_key")), "select * from table",
                    4, 1000, true);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "不可重试异常");
        }
        Assert.assertEquals(1, realRetryTimes.get());
    }

    @Test
    public void testRunSqlTaskWithRetry_不可重试DataxException() throws Exception {
        PowerMockito.spy(OdpsUtil.class);
        final AtomicInteger realRetryTimes = new AtomicInteger(0);
        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                realRetryTimes.addAndGet(1);
                throw DataXException.asDataXException(OdpsWriterErrorCode.RUN_SQL_FAILED, "不可重试异常");
            }
        }).when(OdpsUtil.class, "runSqlTask", (Odps) anyObject(), anyString());

        try {
            OdpsUtil.runSqlTaskWithRetry(
                    new Odps(new AliyunAccount("datax_test_ID", "datax_test_key")), "select * from table",
                    4, 1000, true);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("不可重试异常"));
        }
        Assert.assertEquals(1, realRetryTimes.get());
    }

    @Test
    public void testRunSqlTaskWithRetry_DataxException_可重试() throws Exception {
        PowerMockito.spy(OdpsUtil.class);
        final AtomicInteger realRetryTimes = new AtomicInteger(0);
        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                realRetryTimes.addAndGet(1);
                throw DataXException.asDataXException(OdpsWriterErrorCode.RUN_SQL_ODPS_EXCEPTION, "可重试异常");
                //return null;
            }
        }).when(OdpsUtil.class, "runSqlTask", (Odps) anyObject(), anyString());

        try {
            OdpsUtil.runSqlTaskWithRetry(
                    new Odps(new AliyunAccount("datax_test_ID", "datax_test_key")), "select * from table",
                    4, 1000, true);
        } catch (DataXException e) {
            Assert.assertEquals(e.getErrorCode(), OdpsWriterErrorCode.RUN_SQL_ODPS_EXCEPTION);
        }
        Assert.assertEquals(4, realRetryTimes.get());
    }

    @Test
    public void testRunSqlTaskWithRetry_第一次异常_第二次成功() throws Exception {
        PowerMockito.spy(OdpsUtil.class);
        final AtomicInteger realRetryTimes = new AtomicInteger(0);
        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                realRetryTimes.addAndGet(1);
                if(realRetryTimes.get() ==2) {
                    System.out.println("执行成功，无须重试");
                    return null;
                } else {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.RUN_SQL_ODPS_EXCEPTION, "可重试异常");
                }
                //return null;
            }
        }).when(OdpsUtil.class, "runSqlTask", (Odps) anyObject(), anyString());


        OdpsUtil.runSqlTaskWithRetry(
                new Odps(new AliyunAccount("datax_test_ID", "datax_test_key")), "select * from table",
                4, 1000, true);

        Assert.assertEquals(2, realRetryTimes.get());
    }


}
