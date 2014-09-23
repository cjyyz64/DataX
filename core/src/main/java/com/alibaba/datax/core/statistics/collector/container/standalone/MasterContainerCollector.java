package com.alibaba.datax.core.statistics.collector.container.standalone;

import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.metric.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;

public class MasterContainerCollector extends AbstractContainerCollector {
	private static final Logger LOG = LoggerFactory
			.getLogger(MasterContainerCollector.class);

	public MasterContainerCollector(Configuration configuration) {
		super(configuration);
	}

	@Override
	public void report(Metric nowMetric) {
		LOG.debug(nowMetric.toString());

		LOG.info(MetricStringify.getSnapshot(nowMetric));
	}

	@Override
	public Metric collect() {
		return MetricManager.getMasterMetric();
	}

}
