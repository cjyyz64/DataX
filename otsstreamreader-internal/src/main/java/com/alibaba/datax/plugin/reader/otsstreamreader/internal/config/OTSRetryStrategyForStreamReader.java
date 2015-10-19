package com.alibaba.datax.plugin.reader.otsstreamreader.internal.config;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.OTSErrorCode;
import com.aliyun.openservices.ots.internal.ClientException;
import com.aliyun.openservices.ots.internal.OTSException;
import com.aliyun.openservices.ots.internal.core.OTSRetryStrategy;

import java.util.Arrays;
import java.util.List;

public class OTSRetryStrategyForStreamReader implements OTSRetryStrategy {

    private static int maxRetries = 10;
    private static long retryPauseScaleTimeMillis = 100;
    private static long maxPauseTimeMillis = 10 * 1000;

    private static List<String> noRetryErrorCode = Arrays.asList(
            OTSErrorCode.AUTHORIZATION_FAILURE,
            OTSErrorCode.CONDITION_CHECK_FAIL,
            OTSErrorCode.INVALID_PARAMETER,
            OTSErrorCode.INVALID_PK,
            OTSErrorCode.OBJECT_ALREADY_EXIST,
            OTSErrorCode.OBJECT_NOT_EXIST,
            OTSErrorCode.OUT_OF_COLUMN_COUNT_LIMIT,
            OTSErrorCode.OUT_OF_ROW_SIZE_LIMIT,
            OTSErrorCode.REQUEST_TOO_LARGE
    );

    private boolean canRetry(Exception ex) {
        if (ex instanceof OTSException) {
            if (noRetryErrorCode.contains(((OTSException) ex).getErrorCode())) {
                return false;
            }
            return true;
        } else if (ex instanceof ClientException) {
            return true;
        } else {
            return false;
        }
    }

    public boolean shouldRetry(String action, Exception ex, int retries) {
        if (retries > maxRetries) {
            return false;
        }
        if (canRetry(ex)) {
            return true;
        }
        return false;
    }

    public long getPauseDelay(String action, Exception ex, int retries) {
        return Math.min((int)Math.pow(2, retries) * retryPauseScaleTimeMillis, maxPauseTimeMillis);
    }
}
