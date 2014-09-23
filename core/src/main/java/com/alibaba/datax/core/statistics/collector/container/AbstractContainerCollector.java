package com.alibaba.datax.core.statistics.collector.container;

import com.alibaba.datax.common.util.Configuration;

/**
 * Created by jingxing on 14-9-11.
 */
public abstract class AbstractContainerCollector implements ContainerCollector {
    private Configuration configuration;

    public Configuration getConfiguration() {
        return configuration;
    }

    public AbstractContainerCollector(Configuration configuration) {
        this.configuration = configuration;
    }
}
