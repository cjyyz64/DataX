package com.alibaba.datax.plugin.writer.otswriter.utils;

import com.aliyun.openservices.ots.internal.core.OTSRetryStrategy;


public class DefaultNoRetry implements OTSRetryStrategy {

    @Override
    public boolean shouldRetry(String action, Exception ex, int retries) {
        return false;
    }

    @Override
    public long getPauseDelay(String action, Exception ex, int retries) {
        return 0;
    }
}