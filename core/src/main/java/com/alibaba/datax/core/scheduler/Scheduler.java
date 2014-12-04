package com.alibaba.datax.core.scheduler;

import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;

public interface Scheduler {

	void schedule(List<Configuration> configurations,
                  ContainerCollector frameworkCollector);

}
