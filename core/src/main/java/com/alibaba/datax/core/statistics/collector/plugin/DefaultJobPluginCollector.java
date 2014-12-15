package com.alibaba.datax.core.statistics.collector.plugin;

import com.alibaba.datax.common.plugin.JobPluginCollector;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;

import java.util.List;
import java.util.Map;

/**
 * Created by jingxing on 14-9-9.
 */
public final class DefaultJobPluginCollector implements JobPluginCollector {
    private ContainerCollector jobCollector;

    public DefaultJobPluginCollector(ContainerCollector containerCollector) {
        this.jobCollector = containerCollector;
    }

    @Override
    public Map<String, List<String>> getMessage() {
        Communication totalCommunication = this.jobCollector.collect();
        return totalCommunication.getMessage();
    }

    @Override
    public List<String> getMessage(String key) {
        Communication totalCommunication = this.jobCollector.collect();
        return totalCommunication.getMessage(key);
    }
}
