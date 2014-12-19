package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.core.util.communication.TGCommunicationMapHolder;

public class ProcessInnerCollector extends AbstractCollector {

    public ProcessInnerCollector(Long jobId) {
        super.setJobId(jobId);
    }

    @Override
    public Communication collectFromTaskGroup() {
        return TGCommunicationMapHolder.getJobCommunication();
    }
}
