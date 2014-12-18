package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.core.util.communication.CommunicationManager;
import com.alibaba.datax.dataxservice.face.domain.JobStatus;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MultiProcessReporter extends AbstractReporter {
    private static final Logger LOG = LoggerFactory
            .getLogger(MultiProcessReporter.class);

    // how to init it ? TODO
    private Map<Integer, Communication> taskGroupCommunicationMap;


    @Override
    public void updateJobCommunication(Long jobId, Communication communication) {
        JobStatus jobStatus = new JobStatus();

        jobStatus.setStage(communication.getLongCounter("stage").intValue());
        jobStatus.setTotalRecords(communication.getLongCounter("totalReadRecords"));
        jobStatus.setTotalBytes(communication.getLongCounter("totalReadBytes"));

        jobStatus.setSpeedRecords(communication.getLongCounter("recordSpeed"));
        jobStatus.setSpeedBytes(communication.getLongCounter("byteSpeed"));

        jobStatus.setErrorRecords(communication.getLongCounter("totalErrorRecords"));
        jobStatus.setErrorBytes(communication.getLongCounter("totalErrorBytes"));

        DataxServiceUtil.updateJobInfo(jobId, jobStatus);

        LOG.info(CommunicationManager.Stringify.getSnapshot(communication));
    }

    @Override
    public void updateTGCommication(Integer taskGroupId, Communication communication) {
        Validate.isTrue(taskGroupCommunicationMap.containsKey(
                taskGroupId), String.format("taskGroupCommunicationMap中没有注册taskGroupId[%s]的Communication，无法更新该taskGroup的信息", taskGroupId));

        taskGroupCommunicationMap.put(taskGroupId, communication);
    }
}
