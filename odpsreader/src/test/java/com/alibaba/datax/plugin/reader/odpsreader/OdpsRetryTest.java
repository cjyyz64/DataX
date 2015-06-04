package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;

public class OdpsRetryTest {

    @Test
    public void testDoRead_Ok() throws IOException, TunnelException {
        //mock session
        TableTunnel.DownloadSession mockSession = PowerMockito.mock(TableTunnel.DownloadSession.class);
        RecordSender mockRecordSender = PowerMockito.mock(RecordSender.class);
        ReaderProxy readerProxy = new ReaderProxy(mockRecordSender, mockSession,
                new HashMap<String, OdpsType>(), new ArrayList<Pair<String, ColumnType>>(), "pt=1",
                true, 1, 10, true);

        //mock recordReader
        final RecordReader mockRecordReader = PowerMockito.mock(RecordReader.class);
        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return mockRecordReader;
            }
        }).when(mockSession).openRecordReader(anyLong(), anyLong(), anyBoolean());

        final AtomicInteger retryTimes = new AtomicInteger(0);
        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                retryTimes.addAndGet(1);
                System.out.println("ok");
                return null;
            }
        }).when(mockRecordReader).read();

        readerProxy.doRead();
    }

    @Test
    public void testDoRead_Retry() throws IOException, TunnelException {
        //mock session
        TableTunnel.DownloadSession mockSession = PowerMockito.mock(TableTunnel.DownloadSession.class);
        RecordSender mockRecordSender = PowerMockito.mock(RecordSender.class);
        ReaderProxy readerProxy = new ReaderProxy(mockRecordSender, mockSession,
                new HashMap<String, OdpsType>(), new ArrayList<Pair<String, ColumnType>>(), "pt=1",
                true, 1, 10, true);

        //mock recordReader
        final RecordReader mockRecordReader = PowerMockito.mock(RecordReader.class);
        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return mockRecordReader;
            }
        }).when(mockSession).openRecordReader(anyLong(), anyLong(), anyBoolean());

        final AtomicInteger retryTimes = new AtomicInteger(0);
        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Exception {
                retryTimes.addAndGet(1);
                System.out.println("ok");
                throw new Exception("odps read exception");
                //return null;
            }
        }).when(mockRecordReader).read();

        try {
            readerProxy.doRead();
        } catch (DataXException e) {
            Assert.assertEquals(e.getErrorCode(), OdpsReaderErrorCode.ODPS_READ_EXCEPTION);
        }

        Assert.assertEquals(retryTimes.get(), 10);
    }

    @Test
    public void testDoRead_前两次重试_第三次成功() throws IOException, TunnelException {
        //mock session
        TableTunnel.DownloadSession mockSession = PowerMockito.mock(TableTunnel.DownloadSession.class);
        RecordSender mockRecordSender = PowerMockito.mock(RecordSender.class);
        ReaderProxy readerProxy = new ReaderProxy(mockRecordSender, mockSession,
                new HashMap<String, OdpsType>(), new ArrayList<Pair<String, ColumnType>>(), "pt=1",
                true, 1, 10, true);

        //mock recordReader
        final RecordReader mockRecordReader = PowerMockito.mock(RecordReader.class);
        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return mockRecordReader;
            }
        }).when(mockSession).openRecordReader(anyLong(), anyLong(), anyBoolean());

        final AtomicInteger retryTimes = new AtomicInteger(0);
        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Exception {
                retryTimes.addAndGet(1);
                if(retryTimes.get() == 3) {
                    return null;
                } else {
                    System.out.println("odps read exception");
                    throw new Exception("odps read exception");
                }
                //return null;
            }
        }).when(mockRecordReader).read();

        try {
            readerProxy.doRead();
        } catch (DataXException e) {
            Assert.assertEquals(e.getErrorCode(), OdpsReaderErrorCode.ODPS_READ_EXCEPTION);
        }

        Assert.assertEquals(retryTimes.get(), 3);
    }
}
