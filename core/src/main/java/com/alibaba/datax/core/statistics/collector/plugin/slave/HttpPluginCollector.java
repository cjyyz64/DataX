package com.alibaba.datax.core.statistics.collector.plugin.slave;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.metric.Metric;

/**
 * Created by jingxing on 14-9-9.
 */
public class HttpPluginCollector extends AbstractSlavePluginCollector {

	public HttpPluginCollector(Configuration configuration, Metric metric,
			PluginType type) {
		super(configuration, metric, type);
	}

	@Override
	public void collectDirtyRecord(Record dirtyRecord, Throwable t,
			String errorMessage) {
		super.collectDirtyRecord(dirtyRecord, t, errorMessage);
	}
}
