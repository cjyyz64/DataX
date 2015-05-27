package com.alibaba.datax.core.job.scheduler.processinner;

import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.dataxservice.face.domain.Result;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalScheduler extends ProcessInnerScheduler{
    private static final Logger LOG = LoggerFactory
            .getLogger(LocalScheduler.class);

    public LocalScheduler(AbstractContainerCommunicator containerCommunicator) {
        super(containerCommunicator);
    }

    @Override
    public boolean isJobKilling(Long jobId) {
        Result<Integer> jobInfo = DataxServiceUtil.getJobInfo(jobId);
        if(jobInfo.getData() == null) {
            LOG.warn("获取server端 state == null");
            return false;
        }
        return jobInfo.getData() == State.KILLING.value();
    }
}
