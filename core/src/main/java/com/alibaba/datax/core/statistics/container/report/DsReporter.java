package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.core.util.communication.CommunicationManager;
import com.alibaba.datax.dataxservice.face.domain.JobStatus;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupStatus;

public class DsReporter extends AbstractReporter {
    private Long jobId;

    public DsReporter(Long jobId) {
        this.jobId = jobId;
    }

    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {
        JobStatus jobStatus = new JobStatus();

        jobStatus.setStage(communication.getLongCounter("stage").intValue());
        jobStatus.setTotalRecords(communication.getLongCounter("totalReadRecords"));
        jobStatus.setTotalBytes(communication.getLongCounter("totalReadBytes"));

        jobStatus.setSpeedRecords(communication.getLongCounter("recordSpeed"));
        jobStatus.setSpeedBytes(communication.getLongCounter("byteSpeed"));

        jobStatus.setErrorRecords(communication.getLongCounter("totalErrorRecords"));
        jobStatus.setErrorBytes(communication.getLongCounter("totalErrorBytes"));

        jobStatus.setErrorMessage(communication.getThrowableMessage());
        DataxServiceUtil.updateJobInfo(jobId, jobStatus);
    }

    @Override
    public void reportTGCommunication(Integer taskGroupId, Communication communication) {
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

        DataxServiceUtil.updateTaskGroupInfo(this.jobId, taskGroupId, taskGroupStatus);
    }

}
