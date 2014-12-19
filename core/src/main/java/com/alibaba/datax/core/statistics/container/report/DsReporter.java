package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.core.util.communication.CommunicationManager;
import com.alibaba.datax.dataxservice.face.domain.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DsReporter extends AbstractReporter {
    private static final Logger LOG = LoggerFactory
            .getLogger(DsReporter.class);

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

        DataxServiceUtil.updateJobInfo(jobId, jobStatus);

        LOG.info(CommunicationManager.Stringify.getSnapshot(communication));
    }

}
