package com.alibaba.datax.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public final class RetryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RetryUtil.class);

    static final ThreadPoolExecutor EXECUTOR = createThreadPoolExecutor();

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
    public static <T> T executeWithRetry(Callable<T> callable,
                                         int retryTimes,
                                         long sleepTimeInMilliSecond,
                                         boolean exponential) throws Exception {
        Retry retry = new Retry();
        return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential);
    }

    /**
     * 在外部线程执行并且重试。每次执行需要在timeoutMs内执行完，不然视为失败。
     * <p/>
     * 限制：仅仅能够在阻塞的时候interrupt线程
     *
     * @param callable               实际逻辑
     * @param retryTimes             最大重试次数（>1）
     * @param sleepTimeInMilliSecond 运行失败后休眠对应时间再重试
     * @param exponential            休眠时间是否指数递增
     * @param timeoutMs              callable执行超时时间，毫秒
     * @param <T>                    返回值类型
     * @return
     */
    public static <T> T asyncExecuteWithRetry(Callable<T> callable,
                                              int retryTimes,
                                              long sleepTimeInMilliSecond,
                                              boolean exponential,
                                              long timeoutMs) throws Exception {
        Retry retry = new AsyncRetry(timeoutMs);
        return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential);
    }


    static ThreadPoolExecutor createThreadPoolExecutor() {
        return new ThreadPoolExecutor(0, 5,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>());
    }


    private static class Retry {

        public <T> T doRetry(Callable<T> callable, int retryTimes, long sleepTimeInMilliSecond, boolean exponential)
                throws Exception {

            if (null == callable) {
                throw new IllegalArgumentException("系统编程错误, 入参callable不能为空 ! ");
            }

            if (retryTimes < 1) {
                throw new IllegalArgumentException(String.format(
                        "系统编程错误, 入参retrytime[%d]不能小于1 !", retryTimes));
            }

            Exception saveException = null;
            for (int i = 0; i < retryTimes; i++) {
                try {
                    return call(callable);
                } catch (Exception e) {
                    LOG.debug("Exception when calling callable", e);
                    saveException = e;
                    if (i + 1 < retryTimes && sleepTimeInMilliSecond > 0) {
                        long timeToSleep;
                        if (exponential) {
                            timeToSleep = sleepTimeInMilliSecond * (long) Math.pow(2, i);
                        } else {
                            timeToSleep = sleepTimeInMilliSecond;
                        }

                        try {
                            Thread.sleep(timeToSleep);
                        } catch (InterruptedException ignored) {
                        }
                    }
                }

            }
            throw saveException;
        }

        protected  <T> T call(Callable<T> callable) throws Exception {
            return callable.call();
        }
    }

    private static class AsyncRetry extends Retry {
        private long timeoutMs;

        public AsyncRetry(long timeoutMs) {
            this.timeoutMs = timeoutMs;
        }

        @Override
        protected <T> T call(Callable<T> callable) throws Exception {
            Future<T> future = EXECUTOR.submit(callable);
            try {
                return future.get(timeoutMs, TimeUnit.MILLISECONDS);
            } finally {
                if (!future.isDone()) {
                    LOG.debug("do cancel: " + EXECUTOR.getActiveCount());
                    future.cancel(true);
                }
            }
        }
    }

}
