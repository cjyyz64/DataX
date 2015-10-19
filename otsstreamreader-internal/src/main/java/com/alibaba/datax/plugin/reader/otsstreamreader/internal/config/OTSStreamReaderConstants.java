package com.alibaba.datax.plugin.reader.otsstreamreader.internal.config;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.aliyun.openservices.ots.internal.OTSException;
import com.aliyun.openservices.ots.internal.core.OTSRetryStrategy;

public class OTSStreamReaderConstants {

    public static final long BEFORE_OFFSET_TIME_MILLIS = 10 * TimeUtils.MINUTE_IN_MILLIS;

    public static final long AFTER_OFFSET_TIME_MILLIS = 5 * TimeUtils.MINUTE_IN_MILLIS;

    public static final int STATUS_TABLE_TTL = 30 * TimeUtils.DAY_IN_SEC;

    public static final long MAX_WAIT_TABLE_READY_TIME_MILLIS = 2 * TimeUtils.MINUTE_IN_MILLIS;

    public static final long MAX_ONCE_PROCESS_TIME_MILLIS = 15 * TimeUtils.SECOND_IN_MILLIS;

    public static final long MAIN_THREAD_CHECK_INTERVAL_MILLIS = 10 * TimeUtils.SECOND_IN_MILLIS;

    public static final String CONF = "conf";


    /**
     * StreamClient Config:
     */
    public static final long LEASE_DURATION_TIME_MILLIS = 60 * TimeUtils.SECOND_IN_MILLIS;
}
