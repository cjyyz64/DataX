package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupStatusDto;

import java.util.List;

public class DsCollector extends AbstractCollector {

    public DsCollector(Long jobId) {
        super.setJobId(jobId);
    }


    @Override
    public Communication collectFromTaskGroup() {
        /*List<TaskGroup> taskGroupInJob = DataxServiceUtil.getTaskGroupInJob(super.getJobId()).getData();
        for (TaskGroup taskGroup : taskGroupInJob) {
            LocalTGCommunicationManager.updateTaskGroupCommunication(taskGroup.getTaskGroupId(),
                    DataxServiceUtil.convertTaskGroupToCommunication(taskGroup));
        }
        return LocalTGCommunicationManager.getJobCommunication();*/

        //只需要获取tg状态信息，不需要整个tg信息，预防conifg内容过大，导致ds数据库压力。
        List<TaskGroupStatusDto> taskGroupStatusList = DataxServiceUtil.getTaskGroupStatusInJob(super.getJobId()).getData();

        for (TaskGroupStatusDto taskGroupStatus : taskGroupStatusList) {
            LocalTGCommunicationManager.updateTaskGroupCommunication(taskGroupStatus.getTaskGroupId(),
                    DataxServiceUtil.convertTaskGroupToCommunication(taskGroupStatus));
        }

        return LocalTGCommunicationManager.getJobCommunication();
    }

}
