package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
//    @Test
//    public void testDoRead() throws IOException, TunnelException {
//        TableTunnel.DownloadSession mockSession = PowerMockito.mock(TableTunnel.DownloadSession.class);
//        RecordReader recordReader = mockSession.openRecordReader(1L, 20L);
//
//
//        PowerMockito.doThrow(new Exception("aa")).when(recordReader.read());
//
//        RecordSender mockRecordSender = PowerMockito.mock(RecordSender.class);
//        ReaderProxy readerProxy = new ReaderProxy(mockRecordSender, mockSession,
//                new HashMap<String, OdpsType>(), new ArrayList<Pair<String, ColumnType>>(), "partition",
//                true, 1, 10, true);
//        readerProxy.doRead();
//
//    }
}
