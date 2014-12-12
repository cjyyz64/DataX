package com.alibaba.datax.core.scheduler;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.service.face.domain.Result;
import com.alibaba.datax.service.face.domain.State;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractScheduler implements Scheduler {
    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractScheduler.class);

    private ErrorRecordLimit errorLimit;

    private Long jobId;

    public Long getJobId() {
        return jobId;
    }

    public void schedule(List<Configuration> configurations,
                         ContainerCollector jobCollector) {
        Validate.notNull(configurations,
                "standalone scheduler配置不能为空");
        int jobReportIntervalInMillSec = configurations.get(0).getInt(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL, 10000);

        this.jobId = configurations.get(0).getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        String dataxService = configurations.get(0).getString(
                CoreConstant.DATAX_CORE_DATAXSERVICE_ADDRESS);
        int httpTimeOut = configurations.get(0).getInt(
                CoreConstant.DATAX_CORE_DATAXSERVICE_TIMEOUT, 5000);

        initDataXServiceManager(jobId, dataxService, httpTimeOut);

        errorLimit = new ErrorRecordLimit(configurations.get(0));

        /**
         * 给 taskGroupContainer 的 Communication 注册
         */
        jobCollector.registerCommunication(configurations);

        int totalTasks = calculateTaskCount(configurations);
        startAllTaskGroup(configurations);

        Communication lastJobContainerCommunication = new Communication();
        try {
            while (true) {
                /**
                 * step 1: collect job stat
                 * step 2: getReport info
                 * step 3: errorLimit do check
                 * step 4: dealFailedStat(frameworkCollector, throwable);
                 * step 5: dealSucceedStat(frameworkCollector, lastJobContainerCommunication, totalTasks);
                 * step 6: dealKillingStat(frameworkCollector, totalTasks);
                 * step 7: refresh last job stat, and then sleep for next while
                 *
                 * above step, some ones should report info to DS
                 *
                 */
                Communication nowJobContainerCommunication = jobCollector.collect();
                nowJobContainerCommunication.setTimestamp(System.currentTimeMillis());
                LOG.debug(nowJobContainerCommunication.toString());

                Communication reportCommunication = CommunicationManager
                        .getReportCommunication(nowJobContainerCommunication, lastJobContainerCommunication, totalTasks);
                jobCollector.report(reportCommunication);
                errorLimit.checkRecordLimit(reportCommunication);


                if (reportCommunication.getState() == State.SUCCEEDED) {
                    LOG.info("Scheduler accomplished all tasks.");
                    break;
                }

                if (isJobKilling(this.getJobId())) {
                    dealKillingStat(jobCollector, totalTasks);
                } else if (reportCommunication.getState() == State.FAILED) {
                    dealFailedStat(jobCollector, nowJobContainerCommunication.getThrowable());
                }

                lastJobContainerCommunication = nowJobContainerCommunication;
                Thread.sleep(jobReportIntervalInMillSec);
            }
        } catch (InterruptedException e) {
            // 以 failed 状态退出
            LOG.error("捕获到InterruptedException异常!", e);

            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }

    }

    private void initDataXServiceManager(Long jobId, String dataxService, int httpTimeOut) {
        DataxServiceUtil.setBasicUrl(dataxService);
        DataxServiceUtil.setTimeoutInMilliSeconds(httpTimeOut);
        DataxServiceUtil.setJobId(jobId);
    }

    protected abstract void startAllTaskGroup(List<Configuration> configurations);

    protected abstract void dealFailedStat(ContainerCollector frameworkCollector, Throwable throwable);

    protected abstract void dealKillingStat(ContainerCollector frameworkCollector, int totalTasks);

    private int calculateTaskCount(List<Configuration> configurations) {
        int totalTasks = 0;
        for (Configuration taskGroupConfiguration : configurations) {
            totalTasks += taskGroupConfiguration.getListConfiguration(
                    CoreConstant.DATAX_JOB_CONTENT).size();
        }
        return totalTasks;
    }

    private boolean isJobKilling(Long jobId) {
        Result<Integer> jobInfo = DataxServiceUtil.getJobInfo(jobId);
        return jobInfo.getData().intValue() == State.KILLING.value();
    }
}
