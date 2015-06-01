package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.odpsreader.util.OdpsUtil;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.junit.Test;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;

/**
 * Created by hongjiao.hj on 2015/6/1.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({OdpsUtil.class,Thread.class})
public class OdpsUtilTest {

    @Test
    public void testGetRecordReader() throws IOException, TunnelException, NoSuchFieldException, IllegalAccessException {
        int start = 10;
        int count = 100;
        TableTunnel.DownloadSession mockSession = Mockito.mock(TableTunnel.DownloadSession.class);
        OdpsUtil.MAX_RETRY_TIME =3;

        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                System.out.println("重试第一次");
                throw  new Exception("first exception!");
            }
        }).doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                System.out.println("重试第二次");
                throw new Exception("second exception!");
            }
        }).doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                System.out.println("重试第三次");
                throw new Exception("third exception!");
            }
        })
                .when(mockSession).openRecordReader(anyLong(), anyLong(), anyBoolean());

        try {
            OdpsUtil.getRecordReader(mockSession, start, count, false);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof DataXException);
            Assert.assertTrue(e.getMessage().contains("third exception"));
        }

        System.out.println("ok");
    }

    @Test
    public void testCreateMasterSessionForNonPartitionedTable() {
        OdpsUtil.MAX_RETRY_TIME = 4;
        Odps odps = new Odps(new AliyunAccount("datax_testId", "datax_testKey"));
        long startTime = System.currentTimeMillis();
        try {
            OdpsUtil.createMasterSessionForNonPartitionedTable(odps, "tunnelServer", "odpsProjectName", "tableName");
        } catch (Exception e) {
            System.out.println("ok");
        }
        long endTime = System.currentTimeMillis();
        long sleepInterval = endTime - startTime;
        System.out.println("sleepInterval: " + sleepInterval);
        Assert.assertTrue(sleepInterval > 7000);
    }

    @Test
    public void testGetSlaveSessionForNonPartitionedTable() {
        OdpsUtil.MAX_RETRY_TIME = 4;
        Odps odps = new Odps(new AliyunAccount("datax_testId", "datax_testKey"));
        long startTime = System.currentTimeMillis();
        try {
            OdpsUtil.getSlaveSessionForNonPartitionedTable(odps, "sessionId", "tunnelServer", "odpsProjectName", "tableName");
        } catch (Exception e) {
            System.out.println("ok");
        }
        long endTime = System.currentTimeMillis();
        long sleepInterval = endTime - startTime;
        System.out.println("sleepInterval: " + sleepInterval);
        Assert.assertTrue(sleepInterval > 7000);
    }

    @Test
    public void testCreateMasterSessionForPartitionedTable() {
        OdpsUtil.MAX_RETRY_TIME = 3;
        Odps odps = new Odps(new AliyunAccount("datax_testId", "datax_testKey"));
        long startTime = System.currentTimeMillis();
        try {
            OdpsUtil.createMasterSessionForPartitionedTable(odps, "tunnelServer", "odpsProjectName", "tableName", "pt=1");
        } catch (Exception e) {
            System.out.println("ok");
        }
        long endTime = System.currentTimeMillis();
        long sleepInterval = endTime - startTime;
        System.out.println("sleepInterval: " + sleepInterval);
        Assert.assertTrue(sleepInterval > 3000);
    }

    @Test
    public void testGetSlaveSessionForPartitionedTable() {
        OdpsUtil.MAX_RETRY_TIME = 3;
        Odps odps = new Odps(new AliyunAccount("datax_testId", "datax_testKey"));
        long startTime = System.currentTimeMillis();
        try {
            OdpsUtil.getSlaveSessionForPartitionedTable(odps, "tunnelServer", "sessionId", "odpsProjectName", "tableName", "pt=1");
        } catch (Exception e) {
            System.out.println("ok");
        }
        long endTime = System.currentTimeMillis();
        long sleepInterval = endTime - startTime;
        System.out.println("sleepInterval: " + sleepInterval);
        Assert.assertTrue(sleepInterval > 3000);
    }
}
