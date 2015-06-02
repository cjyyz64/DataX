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

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;

/**
 * Created by hongjiao.hj on 2015/6/1.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({OdpsUtil.class})
public class OdpsUtilTest {

    @Test
    public void testRunSqlTaskWithRetry() {
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
        //PowerMockito.doNothing().when(OdpsUtil.class, "runSqlTask", (Odps)anyObject(), anyString());
        int realRetryTimes = 0;
        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                //realRetryTimes ++;
                return null;
            }
        }).when(OdpsUtil.class, "runSqlTask", (Odps) anyObject(), anyString());
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


}
