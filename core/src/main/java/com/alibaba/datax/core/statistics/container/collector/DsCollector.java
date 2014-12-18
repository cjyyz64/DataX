package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.dataxservice.face.domain.State;
import com.alibaba.datax.dataxservice.face.domain.TaskGroup;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DsCollector extends AbstractCollector {

    private Map<Integer, Communication> taskGroupCommunicationMap;
    private Map<Integer, Communication> taskCommunicationMap;

    @Override
    public void registerTGCommunication(List<Configuration> taskGroupConfigurationList) {
        this.taskGroupCommunicationMap = new ConcurrentHashMap<Integer, Communication>();

        for (Configuration config : taskGroupConfigurationList) {
            int taskGroupId = config.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            this.taskGroupCommunicationMap.put(taskGroupId, new Communication());
        }

    }

    @Override
    public Map<Integer, Communication> registerTaskCommunication(List<Configuration> taskConfigurationList) {
        this.taskCommunicationMap = new ConcurrentHashMap<Integer, Communication>();

        for (Configuration taskConfig : taskConfigurationList) {
            int taskId = taskConfig.getInt(CoreConstant.JOB_TASK_ID);
            this.taskCommunicationMap.put(taskId, new Communication());
        }

        return this.taskCommunicationMap;
    }


    @Override
    public Communication collectFromTask() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskCommunication :
                this.taskCommunicationMap.values()) {
            communication.mergeFrom(taskCommunication);
        }

        return communication;
    }

    @Override
    public Communication collectFromTaskGroup() {
        List<TaskGroup> taskGroupInJob = DataxServiceUtil.getTaskGroupInJob(super.getJobId()).getData();

        for (TaskGroup taskGroup : taskGroupInJob) {
            this.taskGroupCommunicationMap.put(taskGroup.getTaskGroupId(),
                    DataxServiceUtil.convertTaskGroupToCommunication(taskGroup));
        }

        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskGroupCommunication :
                this.taskGroupCommunicationMap.values()) {
            communication.mergeFrom(taskGroupCommunication);
        }

        return communication;
    }
}
