package com.alibaba.datax.core.statistics.collector.container.local;

import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.metric.MetricJsonify;
import com.alibaba.datax.core.statistics.metric.MetricManager;
import com.alibaba.datax.core.statistics.metric.MetricStringify;
import com.alibaba.datax.core.util.CoreConstant;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.metric.Metric;

public class MasterContainerCollector extends AbstractContainerCollector {
	private static final Logger LOG = LoggerFactory
			.getLogger(MasterContainerCollector.class);

	@SuppressWarnings("unused")
	private long masterId;

	private String clusterManagerAddress;

	private int clusterManagertimeout;

	public MasterContainerCollector(Configuration configuration) {
		super(configuration);
		this.masterId = configuration
				.getLong(CoreConstant.DATAX_CORE_CONTAINER_MASTER_ID);
		this.clusterManagerAddress = configuration
				.getString(CoreConstant.DATAX_CORE_CLUSTERMANAGER_ADDRESS);
		this.clusterManagertimeout = configuration
				.getInt(CoreConstant.DATAX_CORE_CLUSTERMANAGER_TIMEOUT,
						3000);
		Validate.isTrue(StringUtils.isNotBlank(this.clusterManagerAddress),
				"在[local container collector]模式下，master的汇报地址不能为空");
	}

	@Override
	public void report(Metric nowMetric) {
		String message = MetricJsonify.getSnapshot(nowMetric);
        LOG.info(MetricStringify.getSnapshot(nowMetric));

		try {
			String result = Request.Put(String.format("%s/job/%d/status", this.clusterManagerAddress, masterId))
					.connectTimeout(this.clusterManagertimeout).socketTimeout(this.clusterManagertimeout)
					.bodyString(message, ContentType.APPLICATION_JSON)
					.execute().returnContent().asString();
			LOG.debug(result);
		} catch (Exception e) {
			LOG.warn("在[local container collector]模式下，master汇报出错: " + e.getMessage());
		}
	}

	@Override
	public Metric collect() {
		return MetricManager.getMasterMetric();
	}
}
