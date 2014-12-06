package com.alibaba.datax.core.scheduler;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scheduler.standalone.TaskGroupContainerRunner;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.State;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractScheduler implements Scheduler {
    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractScheduler.class);

    private List<TaskGroupContainerRunner> taskGroupContainerRunners = new ArrayList<TaskGroupContainerRunner>();
    private ErrorRecordLimit errorLimit;

    public void schedule(List<Configuration> configurations,
                         ContainerCollector frameworkCollector) {
        Validate.notNull(configurations,
                "standalone scheduler配置不能为空");
        int jobReportIntervalInMillSec = configurations.get(0).getInt(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL, 10000);

        errorLimit = new ErrorRecordLimit(configurations.get(0));

        /**
         * 给 taskGroupContainer 的 Communication 注册
         */
        frameworkCollector.registerCommunication(configurations);

        int totalTasks = calculateTaskCount(configurations);
        startAllTaskGroup(configurations);

        Communication lastJobContainerCommunication = new Communication();
        boolean isDone = false;
        try {
            while (true) {
                /**
                 * step 1: collect job stat
                 * step 2: getReport info
                 * step 3: errorLimit do check
                 * step 4: checkAndDealFailedStat(frameworkCollector, totalTasks);
                 * step 5: checkAndDealSucceedStat(frameworkCollector, lastJobContainerCommunication, totalTasks);
                 * step 6: checkAndDealKillingStat(frameworkCollector, totalTasks);
                 * step 7: refresh last job stat, and then sleep for next while
                 *
                 * above step, some ones should report info to DS
                 *
                 */
                Communication nowJobContainerCommunication = frameworkCollector.collect();
                nowJobContainerCommunication.setTimestamp(System.currentTimeMillis());
                LOG.debug(nowJobContainerCommunication.toString());

                Communication reportCommunication = CommunicationManager
                        .getReportCommunication(nowJobContainerCommunication, lastJobContainerCommunication, totalTasks);

                frameworkCollector.report(reportCommunication);
                errorLimit.checkRecordLimit(reportCommunication);

                checkAndDealFailedStat(frameworkCollector, nowJobContainerCommunication, totalTasks);


                if (!hasTaskGroupException(reportCommunication)) {
                    isDone = checkAndDealSucceedStat(frameworkCollector, lastJobContainerCommunication, totalTasks);
                }
                if (isDone) {
                    LOG.info("Scheduler accomplished all jobs.");
                    break;
                }

//                先判断是否为 killing 状态,或者在checkAndDealKillingStat内部进行判断
//                if(nowJobContainerCommunication.getState().isKilling){
//
//                }
                checkAndDealKillingStat(frameworkCollector, totalTasks);

                lastJobContainerCommunication = nowJobContainerCommunication;
                Thread.sleep(jobReportIntervalInMillSec);
            }
        } catch (InterruptedException e) {
            // 以 failed 状态退出
            LOG.error("捕获到InterruptedException异常!", e);

            // TODO report it
            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }

    }

    protected abstract boolean startAllTaskGroup(List<Configuration> configurations);

    protected abstract void checkAndDealFailedStat(ContainerCollector frameworkCollector,
                                                   Communication nowJobContainerCommunication, int totalTasks);

    protected abstract boolean checkAndDealSucceedStat(ContainerCollector frameworkCollector,
                                                       Communication lastJobContainerCommunication, int totalTasks);

    protected abstract void checkAndDealKillingStat(ContainerCollector frameworkCollector, int totalTasks);

    private int calculateTaskCount(List<Configuration> configurations) {
        int totalTasks = 0;
        for (Configuration taskGroupConfiguration : configurations) {
            totalTasks += taskGroupConfiguration.getListConfiguration(
                    CoreConstant.DATAX_JOB_CONTENT).size();
        }
        return totalTasks;
    }

    public boolean hasTaskGroupException(Communication communication) {
        if (!communication.getState().equals(State.SUCCESS)) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_RUNTIME_ERROR,
                    communication.getThrowable());
        }

        return false;
    }
}
