package com.alibaba.datax.plugin.reader.otsstreamreader.internal.config;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.aliyun.openservices.ots.internal.OTSException;
import com.aliyun.openservices.ots.internal.core.OTSRetryStrategy;

public class OTSStreamReaderConstants {

    public static long BEFORE_OFFSET_TIME_MILLIS = 10 * TimeUtils.MINUTE_IN_MILLIS;

    public static long AFTER_OFFSET_TIME_MILLIS = 5 * TimeUtils.MINUTE_IN_MILLIS;

    public static final int STATUS_TABLE_TTL = 30 * TimeUtils.DAY_IN_SEC;

    public static final long MAX_WAIT_TABLE_READY_TIME_MILLIS = 2 * TimeUtils.MINUTE_IN_MILLIS;

    public static final long MAX_OTS_UNAVAILABLE_TIME = 10 * TimeUtils.MINUTE_IN_MILLIS;

    public static final long MAX_ONCE_PROCESS_TIME_MILLIS = MAX_OTS_UNAVAILABLE_TIME;

    public static final long MAIN_THREAD_CHECK_INTERVAL_MILLIS = 5 * TimeUtils.SECOND_IN_MILLIS;

    public static final String CONF = "conf";


    /**
     * StreamClient Config:
     */
    public static final long LEASE_DURATION_TIME_MILLIS = MAX_OTS_UNAVAILABLE_TIME;
    public static final long MAX_DURATION_BEFORE_LAST_SUCCESSFUL_RENEW_OR_TAKE_LEASE = MAX_OTS_UNAVAILABLE_TIME;

    static {
        String beforeOffsetMillis = System.getProperty("BEFORE_OFFSET_TIME_MILLIS");
        if (beforeOffsetMillis != null) {
            BEFORE_OFFSET_TIME_MILLIS = Long.valueOf(beforeOffsetMillis);
        }

        String afterOffsetMillis = System.getProperty("AFTER_OFFSET_TIME_MILLIS");
        if (afterOffsetMillis != null) {
            AFTER_OFFSET_TIME_MILLIS = Long.valueOf(afterOffsetMillis);
        }
    }
}
