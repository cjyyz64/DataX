package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.communication.Communication;

import java.util.List;
import java.util.Map;

public abstract class AbstractCollector {
    private Long jobId;

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public abstract void registerTGCommunication(List<Configuration> taskGroupConfigurationList);

    public abstract Map<Integer,Communication> registerTaskCommunication(List<Configuration> taskConfigurationList);

    public abstract Communication collectFromTask();

    public abstract Communication collectFromTaskGroup();
}
