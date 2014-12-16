package com.alibaba.datax.core.statistics.collector.container.distribute;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.dataxservice.face.domain.JobStatus;
import com.alibaba.datax.dataxservice.face.domain.State;
import com.alibaba.datax.dataxservice.face.domain.TaskGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DistributeJobContainerCollector extends AbstractContainerCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(DistributeJobContainerCollector.class);

    private Map<Integer, Communication> taskGroupCommunicationMap =
            new ConcurrentHashMap<Integer, Communication>();

    private long jobId;

    public DistributeJobContainerCollector(Configuration configuration) {
        super(configuration);
        this.jobId = configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        for (Configuration config : configurationList) {
            int taskGroupId = config.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            this.taskGroupCommunicationMap.put(taskGroupId, new Communication());
        }
    }

    /**
     * 和 LocalJobContainerCollector 的 report 实现一样
     */
    @Override
    public void report(Communication communication) {
        JobStatus jobStatus = new JobStatus();

        jobStatus.setStage(communication.getLongCounter("stage").intValue());
        jobStatus.setTotalRecords(communication.getLongCounter("totalReadRecords"));
        jobStatus.setTotalBytes(communication.getLongCounter("totalReadBytes"));

        jobStatus.setSpeedRecords(communication.getLongCounter("recordSpeed"));
        jobStatus.setSpeedBytes(communication.getLongCounter("byteSpeed"));


        jobStatus.setErrorRecords(communication.getLongCounter("totalErrorRecords"));
        jobStatus.setErrorBytes(communication.getLongCounter("totalErrorBytes"));

        jobStatus.setErrorMessage(communication.getThrowableMessage());

        DataxServiceUtil.updateJobInfo(this.jobId, jobStatus);
        LOG.info(CommunicationManager.Stringify.getSnapshot(communication));
    }

    @Override
    public Communication collect() {
        List<TaskGroup> taskGroupInJob = DataxServiceUtil.getTaskGroupInJob(this.jobId).getData();
        for (TaskGroup taskGroup : taskGroupInJob) {
            taskGroupCommunicationMap.put(taskGroup.getTaskGroupId(),
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

    @Override
    public State collectState() {
        // 注意：这里会通过 this.collect() 再走一次网络
        return this.collect().getState();
    }

    @Override
    public Communication getCommunication(int taskGroupId) {
        /**
         * URL：GET /job/{jobId}/taskGroup/{taskGroupId}
         *
         * 查看对应 taskGroup 的详情，转换为 Communication 即可
         */
        return null;
    }

    @Override
    public List<Communication> getCommunications(List<Integer> taskGroupIds) {
        // TODO 暂时没有地方使用 skip it
        return null;
    }

    @Override
    public Map<Integer, Communication> getCommunicationsMap() {
        return this.taskGroupCommunicationMap;
    }

}
