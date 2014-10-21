package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.internal.OTSRetryStrategy;

public class DefaultRetryStrategy extends OTSRetryStrategy {

    @Override
    public boolean shouldRetry(String action, Exception ex, int retries) {
        if (retries > 3) {
            return false;
        }
        if (action.equals("ListTable") || action.equals("DescribeTable")
                || action.equals("GetRow") || action.equals("BatchGetRow")) {
            return true;
        }
        if (ex instanceof ClientException) {
            return true;
        }
        if (ex instanceof OTSException) {
            int httpStatus = ((OTSException) ex).getHttpStatus();
            if (httpStatus >= 500) {
                return true;
            }
            String errorCode = ((OTSException) ex).getErrorCode();
            if (errorCode.equals("OTSTableNotReady")) {
                return true;
            }
        }
        return false;
    }

}