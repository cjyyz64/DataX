package com.alibaba.datax.core.job.scheduler.processinner;

import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.dataxservice.face.domain.Result;
import com.alibaba.datax.dataxservice.face.domain.State;

/**
 * Created by hongjiao.hj on 2014/12/22.
 */
public class LocalScheduler extends ProcessInnerScheduler{
    public LocalScheduler(AbstractContainerCommunicator containerCommunicator) {
        super(containerCommunicator);
    }

    @Override
    public boolean isJobKilling(Long jobId) {
        Result<Integer> jobInfo = DataxServiceUtil.getJobInfo(jobId);
        return jobInfo.getData() == State.KILLING.value();
    }
}
