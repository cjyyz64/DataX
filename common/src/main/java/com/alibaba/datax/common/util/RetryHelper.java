package com.alibaba.datax.common.util;

import java.util.concurrent.Callable;

public final class RetryHelper {

    /**
     * 重试次数工具方法.
     *
     * @param callable               实际逻辑
     * @param retryTimes             最大重试次数（>1）
     * @param sleepTimeInMilliSecond 运行失败后休眠对应时间再重试
     * @param exponential            休眠时间是否指数递增
     * @param <T>                    返回值类型
     * @return
     */
    public static <T> T executeWithRetry(Callable<T> callable, int retryTimes, long sleepTimeInMilliSecond,
                                         boolean exponential) {
        if (null == callable) {
            throw new IllegalArgumentException("parameter callable can not null");
        }

        if (retryTimes < 1) {
            throw new IllegalArgumentException(String.format("parameter retryTimes can not <1. detail:retryTimes=[%s].",
                    retryTimes));
        }

        Exception saveException = null;
        for (int i = 0; i < retryTimes; i++) {
            try {
                return callable.call();
            } catch (Exception e) {
                saveException = e;
                if (i + 1 < retryTimes) {
                    if (sleepTimeInMilliSecond < 0) {
                        continue;
                    } else {
                        long timeToSleep = 0;
                        if (exponential) {
                            timeToSleep = sleepTimeInMilliSecond * (long) Math.pow(2, i);
                        } else {
                            timeToSleep = sleepTimeInMilliSecond;
                        }
                        try {
                            Thread.sleep(timeToSleep);
                        } catch (InterruptedException unused) {
                        }
                    }
                }
            }
        }
        if (saveException == null) {
            throw new RuntimeException("executeWithRetry failed.");
        }

        throw new RuntimeException(saveException);
    }

}
