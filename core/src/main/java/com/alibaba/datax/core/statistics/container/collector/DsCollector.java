package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.core.util.communication.TGCommunicationMapHolder;
import com.alibaba.datax.dataxservice.face.domain.TaskGroup;

import java.util.List;
import java.util.Map;

public class DsCollector extends AbstractCollector {

    public DsCollector(Long jobId) {
        super.setJobId(jobId);
    }


    @Override
    public Communication collectFromTaskGroup() {
        List<TaskGroup> taskGroupInJob = DataxServiceUtil.getTaskGroupInJob(super.getJobId()).getData();

        for (TaskGroup taskGroup : taskGroupInJob) {
            TGCommunicationMapHolder.updateTaskGroupCommunication(taskGroup.getTaskGroupId(),
                    DataxServiceUtil.convertTaskGroupToCommunication(taskGroup));
        }

        return TGCommunicationMapHolder.getJobCommunication();
    }

    @Override
    public Map<Integer, Communication> getTGCommunicationMap() {
        return null;
    }

}
