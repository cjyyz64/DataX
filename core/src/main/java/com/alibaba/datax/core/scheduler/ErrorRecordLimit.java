package com.alibaba.datax.core.scheduler;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.metric.Metric;
import com.alibaba.datax.core.util.FrameworkErrorCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jingxing on 14-9-15.
 */
public class ErrorRecordLimit {
	private static final Logger LOG = LoggerFactory
			.getLogger(ErrorRecordLimit.class);

	private static long errorRecordsLimit;

	private static boolean needCheck = true;

	public static void setErrorRecordsLimit(double errorLimit) {
		if (errorLimit >= 1) {
			errorRecordsLimit = (long) errorLimit;
			needCheck = true;
		} else {
			needCheck = false;
		}
	}

	public static void checkLimit(Metric metric) {
		if (needCheck) {
			long errorNumber = metric.getTotalReadRecords()
					- metric.getWriteSucceedRecordNumber();
			if (errorRecordsLimit <= errorNumber) {
				LOG.debug(String.format(
						"Error-limit set to %d, error count check .",
						errorRecordsLimit));
				throw new DataXException(
						FrameworkErrorCode.PLUGIN_DIRTY_DATA_LIMIT_EXCEED,
						String.format(
								"Error-limit check failed, limit %d but %d reached in fact .",
								errorRecordsLimit, errorNumber));
			}
		}
	}

}
