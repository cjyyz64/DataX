package com.alibaba.datax.core.statistics.collector.container.local;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.metric.Metric;
import com.alibaba.datax.core.statistics.metric.MetricManager;
import com.alibaba.datax.core.util.CoreConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlaveContainerCollector extends AbstractContainerCollector {
	private static final Logger LOG = LoggerFactory
			.getLogger(SlaveContainerCollector.class);

	private int slaveId;

	public SlaveContainerCollector(Configuration configuration) {
		super(configuration);
		this.slaveId = configuration
				.getInt(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID);
	}

	/**
	 * TODO 目前由于分布式还没有上去，所以先将local的slave report设置为跟standalone一样，
	 * 实际应该反馈给上端http去更新，等分布式上了再改
	 * 
	 * @param nowMetric
	 */
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
