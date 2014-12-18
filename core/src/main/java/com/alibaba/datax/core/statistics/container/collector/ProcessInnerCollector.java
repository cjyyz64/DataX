package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.core.util.communication.LocalTaskGroupCommunicationManager;
import com.alibaba.datax.dataxservice.face.domain.State;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProcessInnerCollector extends AbstractCollector {
    private Map<Integer, Communication> taskCommunicationMap;

    @Override
    public void registerTGCommunication(List<Configuration> taskGroupConfigurationList) {
        for (Configuration config : taskGroupConfigurationList) {
            int taskGroupId = config.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            LocalTaskGroupCommunicationManager.registerTaskGroupCommunication(
                    taskGroupId, new Communication());
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
        return LocalTaskGroupCommunicationManager.getJobCommunication();
    }
}
