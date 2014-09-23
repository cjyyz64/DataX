package com.alibaba.datax.core.statistics.collector.container;

import com.alibaba.datax.core.statistics.metric.Metric;

/**
 * Created by jingxing on 14-9-9.
 */
public interface ContainerCollector {
    void report(Metric nowMetric);

    Metric collect();
}
