package com.alibaba.datax.core.statistics.collector.container.distribute;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.AbstractTaskGroupContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributeTaskGroupContainerCollector extends AbstractTaskGroupContainerCollector {

    private static final Logger LOG = LoggerFactory
            .getLogger(DistributeTaskGroupContainerCollector.class);

    public DistributeTaskGroupContainerCollector(Configuration configuration) {
        super(configuration);
    }


    @Override
    public void report(Communication communication) {
        TaskGroupStatus taskGroupStatus = new TaskGroupStatus();

        taskGroupStatus.setState(communication.getState());
        taskGroupStatus.setTotalRecords(communication.getLongCounter("totalReadRecords"));
        taskGroupStatus.setTotalBytes(communication.getLongCounter("totalReadBytes"));
        taskGroupStatus.setSpeedRecords(communication.getLongCounter(CommunicationManager.RECORD_SPEED));
        taskGroupStatus.setSpeedBytes(communication.getLongCounter(CommunicationManager.BYTE_SPEED));
        taskGroupStatus.setErrorRecords(CommunicationManager.getTotalErrorRecords(communication));
        taskGroupStatus.setErrorBytes(CommunicationManager.getTotalErrorBytes(communication));
        taskGroupStatus.setErrorMessage(communication.getThrowableMessage());

        DataxServiceUtil.updateTaskGroupInfo(super.jobId, super.taskGroupId, taskGroupStatus);
    }

}
