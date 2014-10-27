package com.alibaba.datax.plugin.writer.otswriter.utils;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.OTSException;

public class RetryHelper {
    
    private static final Logger LOG = LoggerFactory.getLogger(RetryHelper.class);
    
    public static <V> V executeWithRetry(Callable<V> callable, int retryTimes, int sleepInMilliSecond) throws Exception {
        int remainingRetryTimes = retryTimes;
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
                remainingRetryTimes = getRetryTimes(e, remainingRetryTimes);
                if (remainingRetryTimes > 0) {  
                    try {   
                        Thread.sleep(sleepInMilliSecond);
                        sleepInMilliSecond += sleepInMilliSecond;
                        if (sleepInMilliSecond > 30000) {
                            sleepInMilliSecond =  30000;
                        }
                    } catch (InterruptedException ee) { 
                        LOG.warn(ee.getMessage());  
                    }
                } else {    
                    LOG.error("Retry times more than limition", e); 
                    throw e;
                }
            }   
        }
    }
    
    public static boolean canRetry(Exception exception) {
        OTSException e = null;
        if (exception instanceof OTSException) {
            e = (OTSException) exception;
            LOG.warn(
                    "OTSException:ErrorCode:{}, ErrorMsg:{}, RequestId:{}", 
                    new Object[]{e.getErrorCode(), e.getMessage(), e.getRequestId()}
                    );
            
            switch (e.getHttpStatus()) {
            case 503:
            case 500:
                return true;
            case 404:
                if (e.getErrorCode().equals(OTSErrorCode.TABLE_NOT_READY)) {
                    return true;
                } else {
                    return false;
                }
            case 403:
                if (e.getErrorCode().equals(OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT) || 
                    e.getErrorCode().equals(OTSErrorCode.QUOTA_EXHAUSTED) ) {
                    return true;
                } else {
                    return false;
                }
            default:
                return false;
            }
            
        } else if (exception instanceof ClientException) {
            ClientException ce = (ClientException) exception;
            LOG.warn(
                    "ClientException:{}, ErrorMsg:{}", 
                    new Object[]{ce.getErrorCode(), ce.getMessage()}
                    );
            return true;
        } else {
            return false;
        }
    }
    
    public static int getRetryTimes(Exception exception, int remainingRetryTimes) throws Exception {
        if (canRetry(exception)) {
            return --remainingRetryTimes;
        } else {
            throw exception;
        }
    }
}
