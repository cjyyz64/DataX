package com.alibaba.datax.plugin.reader.otsreader.unittest;

import static org.junit.Assert.*;

import org.junit.Test;

import com.alibaba.datax.plugin.reader.otsreader.utils.RetryHelper;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.OTSException;

public class RetryHelperUnittest {

    @Test
    public void testGetRetryTimesOTSException() {
        OTSException e = null;
        // 无限重试
        {
            try {
                int remainingRetryTimes = 10;
                e = new OTSException("", null, OTSErrorCode.SERVER_BUSY, "", 503);
                assertEquals(10, RetryHelper.getRetryTimes(e, remainingRetryTimes));

                e = new OTSException("", null, OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "", 403);
                assertEquals(9, RetryHelper.getRetryTimes(e, remainingRetryTimes));

                e = new OTSException("", null, OTSErrorCode.TABLE_NOT_READY, "", 404);
                assertEquals(10, RetryHelper.getRetryTimes(e, remainingRetryTimes));
            } catch (Exception ee) {
                assertTrue(false);
            }
        }
        // 有限重试
        {
            try {
                int remainingRetryTimes = 10;
                e = new OTSException("", null, OTSErrorCode.INTERNAL_SERVER_ERROR, "", 500);
                assertEquals(9, RetryHelper.getRetryTimes(e, remainingRetryTimes));

                e = new OTSException("", null, OTSErrorCode.REQUEST_TIMEOUT, "", 500);
                assertEquals(9, RetryHelper.getRetryTimes(e, remainingRetryTimes));

                e = new OTSException("", null, OTSErrorCode.PARTITION_UNAVAILABLE, "", 500);
                assertEquals(9, RetryHelper.getRetryTimes(e, remainingRetryTimes));

                e = new OTSException("", null, OTSErrorCode.STORAGE_TIMEOUT, "", 500);
                assertEquals(9, RetryHelper.getRetryTimes(e, remainingRetryTimes));

                e = new OTSException("", null, OTSErrorCode.SERVER_UNAVAILABLE, "", 500);
                assertEquals(9, RetryHelper.getRetryTimes(e, remainingRetryTimes));

                e = new OTSException("", null, OTSErrorCode.SERVER_UNAVAILABLE, "", 500);
                assertEquals(0, RetryHelper.getRetryTimes(e, 1));

                e = new OTSException("", null, OTSErrorCode.SERVER_UNAVAILABLE, "", 500);
                assertEquals(-1, RetryHelper.getRetryTimes(e, 0));

                e = new OTSException("", null, OTSErrorCode.SERVER_UNAVAILABLE, "", 500);
                assertEquals(-2, RetryHelper.getRetryTimes(e, -1));
            } catch (Exception ee) {
                assertTrue(false);
            }
        }
        // 不能重试
        {
            int remainingRetryTimes = 10;

            try {
                e = new OTSException("", null, OTSErrorCode.AUTHORIZATION_FAILURE, "", 403);
                RetryHelper.getRetryTimes(e, remainingRetryTimes);
                assertTrue(false);
            } catch (Exception ee){
                assertTrue(true);
            }
            try {
                e = new OTSException("", null, OTSErrorCode.INVALID_PARAMETER, "", 400);
                RetryHelper.getRetryTimes(e, remainingRetryTimes);
                assertTrue(false);
            } catch (Exception ee){
                assertTrue(true);
            }
            try {
                e = new OTSException("", null, OTSErrorCode.REQUEST_TOO_LARGE, "", 400);
                RetryHelper.getRetryTimes(e, remainingRetryTimes);
                assertTrue(false);
            } catch (Exception ee){
                assertTrue(true);
            }
            try {
                e = new OTSException("", null, OTSErrorCode.OBJECT_NOT_EXIST, "", 404);
                RetryHelper.getRetryTimes(e, remainingRetryTimes);
                assertTrue(false);
            } catch (Exception ee){
                assertTrue(true);
            }
            try {
                e = new OTSException("", null, OTSErrorCode.INVALID_PK, "", 400);
                RetryHelper.getRetryTimes(e, remainingRetryTimes);
                assertTrue(false);
            } catch (Exception ee){
                assertTrue(true);
            }
        } 
    }

    @Test
    public void testGetRetryTimesClientException() {
        ClientException e = null;
        // 有限重试
        try {
            int remainingRetryTimes = 10;
            e = new ClientException("SocketTimeout");
            assertEquals(9, RetryHelper.getRetryTimes(e, remainingRetryTimes));
            assertEquals(9, RetryHelper.getRetryTimes(e, remainingRetryTimes));

            assertEquals(0, RetryHelper.getRetryTimes(e, 1));
            assertEquals(-1, RetryHelper.getRetryTimes(e, 0));
            assertEquals(-2, RetryHelper.getRetryTimes(e, -1));
        } catch (Exception ee) {
            assertTrue(false);
        }
    }

    @Test
    public void testGetRetryTimesException() {
        Exception e = null;
        // 不能重试
        try {
            int remainingRetryTimes = 100;
            e = new Exception("TestException");
            RetryHelper.getRetryTimes(e, remainingRetryTimes);
            assertTrue(false);
        } catch (Exception ee) {
            assertTrue(true);
        }
    }
}
