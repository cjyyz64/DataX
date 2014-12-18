package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.util.communication.Communication;

public abstract class AbstractReporter {
    public abstract void updateJobCommunication(Long jobId, Communication communication);

    public abstract void updateTGCommication(Integer taskGroupId, Communication communication);

}
