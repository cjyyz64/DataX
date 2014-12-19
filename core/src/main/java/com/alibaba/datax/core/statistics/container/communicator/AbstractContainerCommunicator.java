package com.alibaba.datax.core.statistics.container.communicator;


import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.common.CoreConstant;
import com.alibaba.datax.core.statistics.container.collector.AbstractCollector;
import com.alibaba.datax.core.statistics.container.report.AbstractReporter;
import com.alibaba.datax.core.util.communication.Communication;
import org.apache.commons.lang3.Validate;

import java.util.Map;

public abstract class AbstractContainerCommunicator implements ContainerCommunicator {
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

    public void setCollector(AbstractCollector collector) {
        this.collector = collector;
    }

    public void setReporter(AbstractReporter reporter) {
        this.reporter = reporter;
    }

    public Long getJobId() {
        return jobId;
    }

    public AbstractContainerCommunicator(Configuration configuration) {
        this.configuration = configuration;
        this.jobId = configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
    }

    @Override
    public Communication getCommunication(Integer taskGroupId) {
        Validate.isTrue(taskGroupId >= 0, "注册的taskGroupId不能小于0");

        return this.getCommunicationMap().get(taskGroupId);
    }

    public Map<Integer, Communication> getTaskCommunicationMap() {
        return this.collector.getTaskCommunicationMap();
    }

}