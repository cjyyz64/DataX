package com.alibaba.datax.core.statistics.collector.plugin.slave;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.SlavePluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.metric.Metric;
import com.alibaba.datax.core.util.FrameworkErrorCode;

/**
 * Created by jingxing on 14-9-11.
 */
public abstract class AbstractSlavePluginCollector extends SlavePluginCollector {
	private static final Logger LOG = LoggerFactory
			.getLogger(AbstractSlavePluginCollector.class);

	private Metric metric;

	private Configuration configuration;

	private PluginType pluginType;

	public AbstractSlavePluginCollector(Configuration conf, Metric metric,
			PluginType type) {
		this.configuration = conf;
		this.metric = metric;
		this.pluginType = type;
	}

	public Metric getMetric() {
		return metric;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public PluginType getPluginType() {
		return pluginType;
	}

	@Override
	final public void collectMessage(String key, String value) {
		this.metric.addMessage(key, value);
	}

	@Override
	public void collectDirtyRecord(Record dirtyRecord, Throwable t,
			String errorMessage) {

		if (null == dirtyRecord) {
			LOG.warn("Dirty record meet null .");
			return;
		}

		if (this.pluginType.equals(PluginType.READER)) {
			this.metric.incrReadFailedRecords(1L);
			this.metric.incrReadFailedBytes(dirtyRecord.getByteSize());
		} else if (this.pluginType.equals(PluginType.WRITER)) {
			this.metric.incrWriteFailedRecords(1L);
			this.metric.incrWriteFailedBytes(dirtyRecord.getByteSize());
		} else {
			throw DataXException.asDataXException(
					FrameworkErrorCode.INNER_ERROR,
					String.format("Unknown plugin type [%s] .", this.pluginType));
		}
	}
}
