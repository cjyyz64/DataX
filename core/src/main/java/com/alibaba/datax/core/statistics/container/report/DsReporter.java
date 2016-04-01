package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.util.ExceptionTracker;
import com.alibaba.datax.dataxservice.face.domain.JobStatisticsDto2;
import com.alibaba.datax.dataxservice.face.domain.JobStatisticsListWapper2;
import com.alibaba.datax.dataxservice.face.domain.JobStatusDto;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupStatusDto;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.ArrayList;

public class DsReporter extends AbstractReporter {

    private static final Logger LOG = LoggerFactory.getLogger(DsReporter.class);

    public static final int MSG_SIZE = 4096;
    private final static DecimalFormat df = new DecimalFormat("0.00");

    private Long jobId;

    public DsReporter(Long jobId) {
        this.jobId = jobId;
    }

    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {
        JobStatusDto jobStatus = new JobStatusDto();

        jobStatus.setStage(communication.getLongCounter("stage").intValue());
        jobStatus.setTotalRecords(communication.getLongCounter("totalReadRecords"));
        jobStatus.setTotalBytes(communication.getLongCounter("totalReadBytes"));

        jobStatus.setSpeedRecords(communication.getLongCounter("recordSpeed"));
        jobStatus.setSpeedBytes(communication.getLongCounter("byteSpeed"));

        jobStatus.setErrorRecords(communication.getLongCounter("totalErrorRecords"));
        jobStatus.setErrorBytes(communication.getLongCounter("totalErrorBytes"));
        jobStatus.setPercentage(communication.getDoubleCounter("percentage"));

        if (communication.getThrowable() != null && communication.getThrowable() instanceof NullPointerException) {
            jobStatus.setErrorMessage(ExceptionTracker.trace(communication.getThrowable()));
        } else {
            String compressedMsg = StrUtil.compressMiddle(communication.getThrowableMessage(), MSG_SIZE, MSG_SIZE);
            jobStatus.setErrorMessage(compressedMsg);
        }
        try {
            DataxServiceUtil.updateJobInfo(jobId, jobStatus);

            JobStatisticsDto2 jobStatistics = PerfTrace.getInstance().getReports("job");

            if(jobStatistics!=null){
                jobStatistics.setRecords(communication.getLongCounter("totalReadRecords"));
                jobStatistics.setBytes(communication.getLongCounter("totalReadBytes"));
                jobStatistics.setSpeedRecord(communication.getLongCounter("recordSpeed"));
                jobStatistics.setSpeedByte(communication.getLongCounter("byteSpeed"));
                jobStatistics.setErrorBytes(communication.getLongCounter("totalErrorBytes"));
                jobStatistics.setErrorRecord(communication.getLongCounter("totalErrorRecords"));
                jobStatistics.setStagePercent(df.format(communication.getDoubleCounter("percentage") * 100) + "%");
                jobStatistics.setWaitReadTimeMs(communication.getLongCounter("waitWriterTime")/1000000);
                jobStatistics.setWaitWriteTimeMs(communication.getLongCounter("waitReaderTime")/1000000);
                DataxServiceUtil.reportDataxPerfLog(getWapper(jobStatistics));
            }

        } catch (Exception e) {
            LOG.error("Exception when report job communication, jobId: " + jobId +
                            ", status: " + JSON.toJSONString(jobStatus),
                    e);
        }


    }

    @Override
    public void reportTGCommunication(Integer taskGroupId, Communication communication) {
        TaskGroupStatusDto taskGroupStatus = new TaskGroupStatusDto();

        // 不能设置 state，否则会收到 DataXService 的报错：State should be updated be alisa ONLY.
        // taskGroupStatus.setState(communication.getState());
        taskGroupStatus.setStage(communication.getLongCounter("stage").intValue());
        taskGroupStatus.setTotalRecords(CommunicationTool.getTotalReadRecords(communication));
        taskGroupStatus.setTotalBytes(CommunicationTool.getTotalReadBytes(communication));

        taskGroupStatus.setSpeedRecords(communication.getLongCounter(CommunicationTool.RECORD_SPEED));
        taskGroupStatus.setSpeedBytes(communication.getLongCounter(CommunicationTool.BYTE_SPEED));

        taskGroupStatus.setErrorRecords(CommunicationTool.getTotalErrorRecords(communication));
        taskGroupStatus.setErrorBytes(CommunicationTool.getTotalErrorBytes(communication));

        if (communication.getThrowable() != null && communication.getThrowable() instanceof NullPointerException) {
            taskGroupStatus.setErrorMessage(ExceptionTracker.trace(communication.getThrowable()));
        } else {
            String compressedMsg = StrUtil.compressMiddle(communication.getThrowableMessage(), MSG_SIZE, MSG_SIZE);
            taskGroupStatus.setErrorMessage(compressedMsg);
        }

        try {
            DataxServiceUtil.updateTaskGroupInfo(this.jobId, taskGroupId, taskGroupStatus);

            JobStatisticsDto2 jobStatistics = PerfTrace.getInstance().getReports("tg");

            if(jobStatistics!=null){
                jobStatistics.setRecords(communication.getLongCounter("totalReadRecords"));
                jobStatistics.setBytes(communication.getLongCounter("totalReadBytes"));
                jobStatistics.setSpeedRecord(communication.getLongCounter("recordSpeed"));
                jobStatistics.setSpeedByte(communication.getLongCounter("byteSpeed"));
                jobStatistics.setErrorBytes(communication.getLongCounter("totalErrorBytes"));
                jobStatistics.setErrorRecord(communication.getLongCounter("totalErrorRecords"));
                jobStatistics.setStagePercent(df.format(communication.getDoubleCounter("percentage") * 100) + "%");
                jobStatistics.setWaitReadTimeMs(communication.getLongCounter("waitWriterTime")/1000000);
                jobStatistics.setWaitWriteTimeMs(communication.getLongCounter("waitReaderTime")/1000000);
                DataxServiceUtil.reportDataxPerfLog(getWapper(jobStatistics));
            }

        } catch (Exception e) {
            LOG.error("Exception when report task group communication, job: " + jobId +
                            ", task group: " + taskGroupId +
                            ", status: " + JSON.toJSONString(taskGroupStatus),
                    e);
        }
    }

    private JobStatisticsListWapper2 getWapper(final JobStatisticsDto2 dto){
        final ArrayList<JobStatisticsDto2> list = new ArrayList<JobStatisticsDto2>();
        list.add(dto);

        return new JobStatisticsListWapper2(){
            {
                this.setJobStatisticsDtoList(list);
            }
        };
    }

}
