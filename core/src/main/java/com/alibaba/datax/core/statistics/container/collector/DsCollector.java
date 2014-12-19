package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.dataxservice.face.domain.TaskGroup;

import java.util.List;

public class DsCollector extends AbstractCollector {

    public DsCollector(Long jobId) {
        super.setJobId(jobId);
    }


    @Override
    public Communication collectFromTaskGroup() {
        List<TaskGroup> taskGroupInJob = DataxServiceUtil.getTaskGroupInJob(super.getJobId()).getData();

        for (TaskGroup taskGroup : taskGroupInJob) {
            LocalTGCommunicationManager.updateTaskGroupCommunication(taskGroup.getTaskGroupId(),
                    DataxServiceUtil.convertTaskGroupToCommunication(taskGroup));
        }

        return LocalTGCommunicationManager.getJobCommunication();
    }

}
