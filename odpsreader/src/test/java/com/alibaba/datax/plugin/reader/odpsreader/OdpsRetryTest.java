package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Date: 2015/5/28 17:01
 *
 * @author Administrator <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class OdpsRetryTest {

//    @Test
//    public void testRetryDoRead() throws Exception {
//        final AtomicInteger retryTime = new AtomicInteger(0);
//        try {
//            OdpsReader.Task odpsReaderTask = new OdpsReader.Task();
//            //mock readerProxy
//            ReaderProxy readerProxy = PowerMockito.mock(ReaderProxy.class);
//            PowerMockito.doAnswer(new Answer<Void>() {
//                @Override
//                public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
//                    retryTime.addAndGet(1);
//                    System.out.println("execute do read.....");
//                    throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_READ_EXCEPTION, "mock read time out...");
//                }
//            }).when(readerProxy).doRead();
//            //execute retry
//            odpsReaderTask.retryDoRead(3, 1000, readerProxy);
//        } catch (Exception e) {
//            assertTrue(e instanceof DataXException);
//            DataXException exception = (DataXException) e;
//            assertEquals(exception.getErrorCode(), OdpsReaderErrorCode.ODPS_READ_EXCEPTION);
//            assertTrue(exception.getMessage().contains("mock read time out"));
//            e.printStackTrace();
//        } finally {
//            assertEquals(retryTime.get(), 3);
//        }
//    }
//
//    @Test
//    public void testRetryDoReadOk() throws Exception {
//        final AtomicInteger retryTime = new AtomicInteger(0);
//        OdpsReader.Task odpsReaderTask = new OdpsReader.Task();
//        ReaderProxy readerProxy = PowerMockito.mock(ReaderProxy.class);
//        PowerMockito.doAnswer(new Answer<Void>() {
//            @Override
//            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
//                System.out.println("execute do read.....");
//                return null;
//            }
//        }).when(readerProxy).doRead();
//        odpsReaderTask.retryDoRead(3, 1000, readerProxy);
//        assertTrue(retryTime.get() == 0);
//    }
//
//    @Test
//    public void testRetryDoReadOk2() throws Exception {
//        final AtomicInteger retryTime = new AtomicInteger(0);
//        try {
//            OdpsReader.Task odpsReaderTask = new OdpsReader.Task();
//            //mock readerProxy
//            ReaderProxy readerProxy = PowerMockito.mock(ReaderProxy.class);
//            PowerMockito.doAnswer(new Answer<Pair<DataXException, Long>>() {
//                @Override
//                public Pair<DataXException, Long> answer(InvocationOnMock invocationOnMock) throws Throwable {
//                    retryTime.addAndGet(1);
//                    System.out.println("execute do read.....");
//                    Pair<DataXException, Long> pair = new ImmutablePair<DataXException, Long>(
//                            DataXException.asDataXException(OdpsReaderErrorCode.ODPS_READ_EXCEPTION, "mock read time out 1..."),
//                            5L);
//                   return pair;
//                }
//            }).doAnswer(new Answer<Pair<DataXException, Long>>() {
//                @Override
//                public Pair<DataXException, Long> answer(InvocationOnMock invocationOnMock) throws Throwable {
//                    retryTime.addAndGet(1);
//                    System.out.println("execute do read.....");
//                    Pair<DataXException, Long> pair = new ImmutablePair<DataXException, Long>(
//                            DataXException.asDataXException(OdpsReaderErrorCode.ODPS_READ_EXCEPTION, "mock read time out 2..."),
//                            5L);
//                    return pair;
//                }
//            }).doAnswer(new Answer<Pair<DataXException, Long>>() {
//                @Override
//                public Pair<DataXException, Long> answer(InvocationOnMock invocationOnMock) throws Throwable {
//                    retryTime.addAndGet(1);
//                    System.out.println("execute do read.....");
//                    Pair<DataXException, Long> pair = new ImmutablePair<DataXException, Long>(
//                            DataXException.asDataXException(OdpsReaderErrorCode.ODPS_READ_EXCEPTION, "mock read time out 3..."),
//                            6L);
//                    return pair;
//                }
//            }).when(readerProxy).doRead();
//            //execute retry
//            odpsReaderTask.retryDoRead(3, 1000, readerProxy);
//        } catch (Exception e) {
//            assertTrue(e instanceof DataXException);
//            DataXException exception = (DataXException) e;
//            assertEquals(exception.getErrorCode(), OdpsReaderErrorCode.ODPS_READ_EXCEPTION);
//            assertTrue(exception.getMessage().contains("mock read time out 3"));
//            e.printStackTrace();
//        } finally {
//            assertEquals(retryTime.get(), 5);
//        }
//    }
    @Test
    public void testgg() {
        System.out.println("ok");
    }

}
