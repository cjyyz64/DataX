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

    // 用于计算 speed
    private Communication oldCommuication;

    /**
     * 注意：这里的 report，是用于 每一个 taskGroup 搜集自身对应的 task 的状态，然后汇报到 DataxService.
     */
    @Override
    public void report(Communication communication) {
        TaskGroupStatus taskGroupStatus = new TaskGroupStatus();

        // 不能设置 state，否则会收到 DataXService 的报错：State should be updated be alisa ONLY.
        // taskGroupStatus.setState(communication.getState());
        taskGroupStatus.setStage(communication.getLongCounter("stage").intValue());
        taskGroupStatus.setTotalRecords(CommunicationManager.getTotalReadRecords(communication));
        taskGroupStatus.setTotalBytes(CommunicationManager.getTotalReadBytes(communication));

        taskGroupStatus.setSpeedRecords(communication.getLongCounter(CommunicationManager.RECORD_SPEED));
        taskGroupStatus.setSpeedBytes(communication.getLongCounter(CommunicationManager.BYTE_SPEED));

        taskGroupStatus.setErrorRecords(CommunicationManager.getTotalErrorRecords(communication));
        taskGroupStatus.setErrorBytes(CommunicationManager.getTotalErrorBytes(communication));
        taskGroupStatus.setErrorMessage(communication.getThrowableMessage());

        DataxServiceUtil.updateTaskGroupInfo(super.jobId, super.taskGroupId, taskGroupStatus);
    }

}
