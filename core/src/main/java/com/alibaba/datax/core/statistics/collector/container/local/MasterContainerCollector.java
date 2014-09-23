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

	private String masterReportAddress;

	private int timeout;

	public MasterContainerCollector(Configuration configuration) {
		super(configuration);
		this.masterId = configuration
				.getLong(CoreConstant.DATAX_CORE_CONTAINER_MASTER_ID);
		this.masterReportAddress = configuration
				.getString(CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_CONTAINER_MASTERREPORTADDRESS);
		this.timeout = configuration
				.getInt(CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_CONTAINER_REPORTTIMEOUT,
						1000);
		Validate.isTrue(StringUtils.isNotBlank(this.masterReportAddress),
				"master report address can not be blank for local container collector");
	}

	@Override
	public void report(Metric nowMetric) {
		String message = MetricJsonify.getSnapshot(nowMetric);
        LOG.info(MetricStringify.getSnapshot(nowMetric));

		try {
			// TODO 这里可能需要组装masterId到address里面来
			String result = Request.Put(this.masterReportAddress)
					.connectTimeout(this.timeout).socketTimeout(this.timeout)
					.bodyString(message, ContentType.APPLICATION_JSON)
					.execute().returnContent().asString();
			LOG.debug(result);
		} catch (Exception e) {
			LOG.warn("Report problem: " + e.getMessage());
		}
	}

	@Override
	public Metric collect() {
		return MetricManager.getMasterMetric();
	}
}
