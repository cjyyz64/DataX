package com.alibaba.datax.core.statistics.collector.plugin;

import com.alibaba.datax.common.plugin.MasterPluginCollector;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.metric.Metric;

import java.util.List;
import java.util.Map;

/**
 * Created by jingxing on 14-9-9.
 */
public final class DefaultMasterPluginCollector implements MasterPluginCollector {
    private ContainerCollector masterContainerCollector;

    public DefaultMasterPluginCollector(ContainerCollector containerCollector) {
        this.masterContainerCollector = containerCollector;
    }

    @Override
    public Map<String, List<String>> getMessage() {
        Metric totalMetric = this.masterContainerCollector.collect();
        return totalMetric.getMessage();
    }

    @Override
    public List<String> getMessage(String key) {
        Metric totalMetric = this.masterContainerCollector.collect();
        return totalMetric.getMessage(key);
    }
}
