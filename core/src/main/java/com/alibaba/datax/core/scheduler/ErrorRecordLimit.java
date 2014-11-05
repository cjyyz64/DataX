package com.alibaba.datax.core.scheduler;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.metric.Metric;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jingxing on 14-9-15.
 */

/**
 * 检查任务是否到达错误记录限制。有检查条数（recordLimit）和百分比(percentageLimit)两种方式。
 * 1. errorRecord表示出错条数不能大于限制数，当超过时任务失败。比如errorRecord为0表示不容许任何脏数据。
 * 2. errorPercentage表示出错比例，在任务结束时校验。
 * 3. errorRecord优先级高于errorPercentage。
 */
public class ErrorRecordLimit {
    private static final Logger LOG = LoggerFactory
            .getLogger(ErrorRecordLimit.class);

    private Long recordLimit;
    private Double percentageLimit;

    public ErrorRecordLimit(Configuration configuration) {
        this(configuration.getLong(CoreConstant.DATAX_JOB_SETTING_ERRORLIMIT_RECORD),
                configuration.getDouble(CoreConstant.DATAX_JOB_SETTING_ERRORLIMIT_PERCENT));
    }

    public ErrorRecordLimit(Long rec, Double percentage) {
        recordLimit = rec;
        percentageLimit = percentage;

        if (percentageLimit != null) {
            Validate.isTrue(0.0 < percentageLimit && percentageLimit < 1.0,
                    "Error limit percentage should be in range (0.0, 1.0)");
        }

        if (recordLimit != null) {
            Validate.isTrue(recordLimit >= 0,
                    "Error limit percentage should be in range (0.0, 1.0)");

            // errorRecord优先级高于errorPercentage.
            percentageLimit = null;
        }
    }

    public void checkRecordLimit(Metric metric) {

        if (recordLimit == null) {
            return;
        }

        long errorNumber = metric.getErrorRecords();
        if (recordLimit < errorNumber) {
            LOG.debug(String.format(
                    "Error-limit set to %d, error count check .",
                    recordLimit));
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_DIRTY_DATA_LIMIT_EXCEED,
                    String.format(
                            "Error-limit check failed, limit %d but %d reached in fact .",
                            recordLimit, errorNumber));
        }
    }

    public void checkPercentageLimit(Metric masterMetric) {

        if (percentageLimit == null) {
            return;
        }
        LOG.debug(String.format(
                "Error-limit set to %f, error percent check .", percentageLimit));

        long total = masterMetric.getTotalReadRecords();
        long error = masterMetric.getErrorRecords();

        if (total > 0 && ((double) error / (double) total) > percentageLimit) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_DIRTY_DATA_LIMIT_EXCEED,
                    String.format(
                            "Error-limit check failed, limit %f but %f reached in fact .",
                            percentageLimit, ((double) error / (double) total)));
        }
    }
}
