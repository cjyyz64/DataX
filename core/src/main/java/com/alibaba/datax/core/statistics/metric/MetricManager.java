package com.alibaba.datax.core.statistics.metric;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.Status;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;

/**
 * Created by jingxing on 14-8-27.
 * 
 * 全局（进程内）单例统计，实际是slave汇报状态的集中地，但standalone模式下，它也是master获取slave状态的源
 */
public final class MetricManager {
	private static final Logger LOG = LoggerFactory
			.getLogger(MetricManager.class);

	// 第一级 key为 slaveId，第二级 key为 channelId
	private static Map<Long, Map<Long, Metric>> CHANNEL_METRICS = new HashMap<Long, Map<Long, Metric>>();

	// 单机汇报缓存
	private static Map<Long, Metric> SLAVE_METRICS = new HashMap<Long, Metric>();

	private MetricManager() {
	}

	public static synchronized Metric registerMetric(long slaveId,
			long channelId) {
		Validate.isTrue(slaveId >= 0 && channelId >= 0,
				"Illegal parameter: slaveId and channelId can not be less than 0.");

		Map<Long, Metric> slaveMetricMap = CHANNEL_METRICS.get(slaveId);
		if (null == slaveMetricMap) {
			slaveMetricMap = new HashMap<Long, Metric>();
			CHANNEL_METRICS.put(slaveId, slaveMetricMap);
		}

		Metric channelMetric = slaveMetricMap.get(channelId);
		if (null == channelMetric) {
			channelMetric = new Metric();
			slaveMetricMap.put(channelId, channelMetric);
		}

		return channelMetric;
	}

	public static synchronized Metric getMasterMetric() {
		Metric masterMetric = new Metric();

		for (long slaveId : SLAVE_METRICS.keySet()) {
			masterMetric.mergeFrom(SLAVE_METRICS.get(slaveId));
		}

		masterMetric.setStatus(getMasterStatus());
		return masterMetric;
	}

	public static synchronized void reportSlaveMetric(long slaveId,
			final Metric metric) {
		MetricManager.SLAVE_METRICS.put(slaveId, metric);
	}

	public static synchronized Metric getSlaveMetricBySlaveId(long slaveId) {
		Metric slaveMetric = new Metric();

		Map<Long, Metric> slaveStatics = CHANNEL_METRICS.get(slaveId);
		if (null == slaveStatics) {
			throw new DataXException(FrameworkErrorCode.INNER_ERROR,
					String.format(
							"Can not get unregistered metric for slaveId:[%d]",
							slaveId));
		}

		for (Entry<Long, Metric> entry : slaveStatics.entrySet()) {
			Metric v = entry.getValue();
			slaveMetric.mergeFrom(v);
		}

		return slaveMetric;
	}

	public static synchronized Map<Long, Metric> getAllSlaveMetric() {
		Map<Long, Metric> slaveMetricMap = new HashMap<Long, Metric>();

		for (Entry<Long, Map<Long, Metric>> entry : CHANNEL_METRICS.entrySet()) {
			Long slaveId = entry.getKey();
			slaveMetricMap.put(slaveId, getSlaveMetricBySlaveId(slaveId));
		}

		return slaveMetricMap;
	}

	public static synchronized Metric getChannelMetric(long slaveId,
			long channelId) {
		Validate.isTrue(slaveId >= 0 && channelId >= 0,
				"Illegal parameter: slaveId and channelId can not be less than 0.");

		Map<Long, Metric> slaveMetricMap = CHANNEL_METRICS.get(slaveId);
		if (null == slaveMetricMap) {
			throw new DataXException(
					FrameworkErrorCode.INNER_ERROR,
					String.format(
							"Can not get unregistered metric for slaveId:[%d] .",
							slaveId));
		}

		Metric channelMetric = slaveMetricMap.get(channelId);
		if (null == channelMetric) {
			throw new DataXException(
					FrameworkErrorCode.INNER_ERROR,
					String.format(
							"Can not get unregistered metric for slaveId:[%d] channelId:[%d] .",
							slaveId, channelId));
		}

		return channelMetric;
	}

	private static synchronized Status getMasterStatus() {
		for (Entry<Long, Map<Long, Metric>> entry : CHANNEL_METRICS.entrySet()) {
			long slaveId = entry.getKey();
			Map<Long, Metric> slaveMetricMap = entry.getValue();
			for (Entry<Long, Metric> innerEntry : slaveMetricMap.entrySet()) {
				long channelId = innerEntry.getKey();
				Metric channelMetric = innerEntry.getValue();
				if (channelMetric.getStatus() == Status.FAIL) {
					LOG.error(String.format(
							"slaveId[%d] channelId[%d] failed, reason: %s",
							slaveId, channelId, channelMetric.getErrorMessage()));
					return Status.FAIL;
				}
			}
		}

		for (Entry<Long, Map<Long, Metric>> entry : CHANNEL_METRICS.entrySet()) {
			Map<Long, Metric> slaveMetricMap = entry.getValue();
			for (Entry<Long, Metric> innerEntry : slaveMetricMap.entrySet()) {
				Metric channelMetric = innerEntry.getValue();
				Status status = channelMetric.getStatus();
				switch (status) {
				case RUN:
					return Status.RUN;
				default:
					break;
				}
			}
		}

		return Status.SUCCESS;
	}

	public static synchronized Status getSlaveStatusBySlaveId(long slaveId) {
		Map<Long, Metric> slaveMetric = CHANNEL_METRICS.get(slaveId);
		if (null == slaveMetric) {
			return Status.RUN;
		}

		for (Entry<Long, Metric> entry : slaveMetric.entrySet()) {
			Metric channelMetric = entry.getValue();
			Status status = channelMetric.getStatus();
			if (status == Status.FAIL) {
				return Status.FAIL;
			}
		}

		for (Entry<Long, Metric> entry : slaveMetric.entrySet()) {
			Metric channelMetric = entry.getValue();
			Status status = channelMetric.getStatus();
			switch (status) {
			case RUN:
				return Status.RUN;
			default:
				break;
			}
		}

		return Status.SUCCESS;
	}

	public static Metric getReportMetric(Metric now, Metric old, int totalStage) {
		Validate.isTrue(now != null || old != null,
				"now and old metric not null for report metric");

		long timeInterval = now.getTimeStamp() - old.getTimeStamp();
		long sec = timeInterval <= 1000 ? 1 : timeInterval / 1000;
		long bytes = (now.getTotalReadBytes() - old.getTotalReadBytes()) / sec;
		long records = (now.getTotalReadRecords() - old.getTotalReadRecords())
				/ sec;
		now.setByteSpeed(bytes);
		now.setRecordSpeed(records);
		now.setPercentage(now.getStage() / totalStage);
		return now;
	}
}
