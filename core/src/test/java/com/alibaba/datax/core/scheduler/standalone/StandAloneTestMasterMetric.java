package com.alibaba.datax.core.scheduler.standalone;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.metric.Metric;

/**
 * Created by jingxing on 14-9-4.
 */
public class StandAloneTestMasterMetric extends AbstractContainerCollector {
    public StandAloneTestMasterMetric(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void report(Metric nowMetric) {
        System.out.println("master report 2");
    }

    @Override
    public Metric collect() {
        return new Metric();
    }
}
