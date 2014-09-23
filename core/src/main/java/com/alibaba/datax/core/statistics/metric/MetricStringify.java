package com.alibaba.datax.core.statistics.metric;

import com.alibaba.datax.common.util.StrUtil;

import java.text.DecimalFormat;

/**
 * Created by jingxing on 14-8-28.
 */
public class MetricStringify {
	private final static DecimalFormat df = new DecimalFormat("0.00");

	public static String getTotalBytes(Metric statistics) {
		return String.format("%d bytes", statistics.getReadFailedBytes()
				+ statistics.getReadSucceedBytes());
	}

	public static String getTotalRecords(Metric statistics) {
		return String.format("%d records", statistics.getReadFailedRecords()
				+ statistics.getReadSucceedRecords());
	}

	public static String getSpeed(Metric now) {
		return String.format("%s/s, %d records/s",
				StrUtil.stringify(now.getByteSpeed()), now.getRecordSpeed());
	}

	public static String getErrorRecord(Metric metric) {
		return String.format("%d records", metric.getReadFailedRecords()
				+ metric.getWriteFailedRecords());
	}

	public static String getSnapshot(Metric now) {
		StringBuilder sb = new StringBuilder(256);
		sb.append("Total ");
		sb.append(getTotalRecords(now));
		sb.append(", ");
		sb.append(getTotalBytes(now));
		sb.append(" | ");
		sb.append("Speed ");
		sb.append(getSpeed(now));
		sb.append(" | ");
		sb.append("Error ");
		sb.append(getErrorRecord(now));
		sb.append(" | ");
		sb.append("Stage ");
		sb.append(getPercentage(now));
		return sb.toString();
	}

	public static String getPercentage(Metric now) {
		return df.format(now.getPercentage() * 100) + "%";
	}
}
