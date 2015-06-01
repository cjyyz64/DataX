package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.exception.DataXException;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;

/**
 * Date: 2015/5/28 17:01
 *
 * @author Administrator <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class OdpsRetryTest {

    @Test
    public void testRetryDoRead() throws Exception {
        final AtomicInteger retryTime = new AtomicInteger(0);
        try {
            OdpsReader.Task odpsReaderTask = new OdpsReader.Task();
            //mock readerProxy
            ReaderProxy readerProxy = PowerMockito.mock(ReaderProxy.class);
            PowerMockito.doAnswer(new Answer<Void>() {
                @Override
                public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                    retryTime.addAndGet(1);
                    System.out.println("execute do read.....");
                    throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_READ_TIMEOUT, "mock read time out...");
                }
            }).when(readerProxy).doRead(anyInt());
            //execute retry
            odpsReaderTask.retryDoRead(3, 1000, readerProxy);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            DataXException exception = (DataXException) e;
            assertEquals(exception.getErrorCode(), OdpsReaderErrorCode.ODPS_READ_TIMEOUT);
            assertTrue(exception.getMessage().contains("mock read time out"));
            e.printStackTrace();
        } finally {
            assertEquals(retryTime.get(), 3);
        }
    }

    @Test
    public void testRetryDoReadOk() throws Exception {
        final AtomicInteger retryTime = new AtomicInteger(0);
        OdpsReader.Task odpsReaderTask = new OdpsReader.Task();
        ReaderProxy readerProxy = PowerMockito.mock(ReaderProxy.class);
        PowerMockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                System.out.println("execute do read.....");
                return null;
            }
        }).when(readerProxy).doRead(anyInt());
        odpsReaderTask.retryDoRead(3, 1000, readerProxy);
        assertTrue(retryTime.get() == 0);
    }
}
