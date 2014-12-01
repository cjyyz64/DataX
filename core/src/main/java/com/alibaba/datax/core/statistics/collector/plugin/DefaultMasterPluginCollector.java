package com.alibaba.datax.core.statistics.collector.plugin;

import com.alibaba.datax.common.plugin.MasterPluginCollector;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;

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
        Communication totalCommunication = this.masterContainerCollector.collect();
        return totalCommunication.getMessage();
    }

    @Override
    public List<String> getMessage(String key) {
        Communication totalCommunication = this.masterContainerCollector.collect();
        return totalCommunication.getMessage(key);
    }
}
