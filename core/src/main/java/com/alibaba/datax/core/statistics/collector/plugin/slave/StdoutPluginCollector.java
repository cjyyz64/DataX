package com.alibaba.datax.core.statistics.collector.plugin.slave;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.statistics.metric.Metric;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jingxing on 14-9-9.
 */
public class StdoutPluginCollector extends AbstractSlavePluginCollector {
	private static final Logger LOG = LoggerFactory
			.getLogger(StdoutPluginCollector.class);

	private AtomicInteger maxLogNum = new AtomicInteger(0);

	private AtomicInteger currentLogNum = new AtomicInteger(0);

	public StdoutPluginCollector(Configuration configuration, Metric metric,
			PluginType type) {
		super(configuration, metric, type);
		maxLogNum = new AtomicInteger(
				configuration
						.getInt(CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_MAXDIRTYNUM,
								100));
	}

	private String formatDirty(final Record dirty, final Throwable t,
			final String msg) {
		StringBuilder sb = new StringBuilder();
		sb.append(dirty == null ? "" : "Bad Record: " + dirty.toString() + "\n");
		sb.append(t == null ? "" : "Java Exception: " + t.getMessage() + "\n");
		sb.append(StringUtils.isBlank(msg) ? "" : "Error Tip: " + msg);
		return sb.toString();
	}

	@Override
	public void collectDirtyRecord(Record dirtyRecord, Throwable t,
			String errorMessage) {
		super.collectDirtyRecord(dirtyRecord, t, errorMessage);

		currentLogNum.incrementAndGet();
		if (currentLogNum.intValue() < maxLogNum.intValue()) {
			LOG.error("\n" + this.formatDirty(dirtyRecord, t, errorMessage));
		}
	}
}
