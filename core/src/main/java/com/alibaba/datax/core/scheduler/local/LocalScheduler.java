package com.alibaba.datax.core.scheduler.local;

import java.util.List;

import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scheduler.Scheduler;

/**
 * Created by jingxing on 14-8-24.
 */
public class LocalScheduler implements Scheduler {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory
			.getLogger(LocalScheduler.class);

    @Override
    public void schedule(List<Configuration> configurations,
                         ContainerCollector frameworkCollector) {

    }

}
