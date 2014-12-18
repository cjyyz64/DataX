package com.alibaba.datax.core.statistics.container;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.collector.AbstractCollector;
import com.alibaba.datax.core.statistics.container.report.AbstractReporter;
import com.alibaba.datax.core.util.CoreConstant;

public abstract class AbstractContainerCollector implements ContainerCollector {
    private Configuration configuration;
    private AbstractCollector collector;
    private AbstractReporter reporter;

    private Long jobId;


    public Configuration getConfiguration() {
        return this.configuration;
    }

    public AbstractCollector getCollector() {
        return collector;
    }

    public AbstractReporter getReporter() {
        return reporter;
    }

    public Long getJobId() {
        return jobId;
    }

    public AbstractContainerCollector(Configuration configuration, AbstractCollector collector, AbstractReporter reporter_temp) {
        this.configuration = configuration;
        this.collector = collector;
        this.reporter = reporter_temp;

        this.jobId = configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
    }

}