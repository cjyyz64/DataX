package com.alibaba.datax.core.statistics.collector.container.standalone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.metric.Metric;
import com.alibaba.datax.core.statistics.metric.MetricManager;
import com.alibaba.datax.core.util.CoreConstant;

public class SlaveContainerCollector extends AbstractContainerCollector {
	private static final Logger LOG = LoggerFactory
			.getLogger(SlaveContainerCollector.class);

	private int slaveId;

	public SlaveContainerCollector(Configuration configuration) {
		super(configuration);
		this.slaveId = configuration
				.getInt(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID);

	}

	@Override
	public void report(Metric nowMetric) {
		LOG.debug("Slave collector: \n" + nowMetric.toString());
		MetricManager.reportSlaveMetric(slaveId, nowMetric);
	}

	@Override
	public Metric collect() {
		return MetricManager.getSlaveMetricBySlaveId(this.slaveId);
	}

}
