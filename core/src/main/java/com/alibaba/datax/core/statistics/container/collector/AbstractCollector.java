package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.common.CoreConstant;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.core.util.communication.TGCommunicationMapHolder;
import com.alibaba.datax.dataxservice.face.domain.State;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractCollector {
    private Map<Integer, Communication> taskCommunicationMap = new ConcurrentHashMap<Integer, Communication>();
    private Long jobId;

    public Map<Integer, Communication> getTaskCommunicationMap() {
        return taskCommunicationMap;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public void registerTGCommunication(List<Configuration> taskGroupConfigurationList) {
        for (Configuration config : taskGroupConfigurationList) {
            int taskGroupId = config.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            TGCommunicationMapHolder.registerTaskGroupCommunication(taskGroupId, new Communication());
        }
    }

    public void registerTaskCommunication(List<Configuration> taskConfigurationList) {
        for (Configuration taskConfig : taskConfigurationList) {
            int taskId = taskConfig.getInt(CoreConstant.JOB_TASK_ID);
            this.taskCommunicationMap.put(taskId, new Communication());
        }
    }

    public Communication collectFromTask() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskCommunication :
                this.taskCommunicationMap.values()) {
            communication.mergeFrom(taskCommunication);
        }

        return communication;
    }

    public abstract Communication collectFromTaskGroup();
}
