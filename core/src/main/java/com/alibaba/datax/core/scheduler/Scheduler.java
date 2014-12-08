package com.alibaba.datax.core.scheduler;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;

import java.util.List;

public interface Scheduler {

	void schedule(List<Configuration> configurations,
                  ContainerCollector jobCollector);

}
