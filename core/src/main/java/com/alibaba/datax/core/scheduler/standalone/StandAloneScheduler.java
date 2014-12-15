package com.alibaba.datax.core.scheduler.standalone;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.TaskGroupContainer;
import com.alibaba.datax.core.scheduler.ErrorRecordLimit;
import com.alibaba.datax.core.scheduler.Scheduler;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.State;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 * 该类是工具包模式下的调度类，它是通过job起多线程来运行taskGroup作业的，除了调度方式和其他模式不一致外，
 * 它的状态汇报也比较特殊，由于在同一个进程中，状态和统计可以在一个全局单粒中提交，job也可以从该单粒中
 * 获取这些信息。但整个架构还是和其他模式保持一致性
 */
public class StandAloneScheduler implements Scheduler {
    private static final Logger LOG = LoggerFactory
            .getLogger(StandAloneScheduler.class);

    private List<TaskGroupContainerRunner> taskGroupContainerRunners = new ArrayList<TaskGroupContainerRunner>();
    private ErrorRecordLimit errorLimit;

    @Override
    public void schedule(List<Configuration> configurations,
                         ContainerCollector frameworkCollector) {
        Validate.notNull(configurations,
                "standalone scheduler配置不能为空");

        int jobReportIntervalInMillSec = configurations.get(0).getInt(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL, 10000);

        errorLimit = new ErrorRecordLimit(configurations.get(0));

        /**
         * 给taskGroupContainer的Communication注册
         */

        frameworkCollector.registerCommunication(configurations);

        ExecutorService taskGroupContainerExecutorService = Executors
                .newFixedThreadPool(configurations.size());

        /**
         * 完成一个task算一个stage，所以这里求所有tasks的和
         */
        int totalTasks = 0;
        for (Configuration taskGroupConfiguration : configurations) {
            TaskGroupContainerRunner taskGroupContainerRunner = newTaskGroupContainerRunner(taskGroupConfiguration);
            totalTasks += taskGroupConfiguration.getListConfiguration(
                    CoreConstant.DATAX_JOB_CONTENT).size();
            taskGroupContainerExecutorService.execute(taskGroupContainerRunner);
            taskGroupContainerRunners.add(taskGroupContainerRunner);
        }
        taskGroupContainerExecutorService.shutdown();

        Communication lastJobContainerCommunication = new Communication();
        lastJobContainerCommunication.setTimestamp(System.currentTimeMillis());
        try {
            do {
                Communication nowJobContainerCommunication = frameworkCollector.collect();
                nowJobContainerCommunication.setTimestamp(System.currentTimeMillis());
                LOG.debug(nowJobContainerCommunication.toString());

                if (nowJobContainerCommunication.getState() == State.FAIL) {
                    taskGroupContainerExecutorService.shutdownNow();
                    throw DataXException.asDataXException(
                            FrameworkErrorCode.PLUGIN_RUNTIME_ERROR,
                            nowJobContainerCommunication.getThrowable());
                }

                Communication reportCommunication = CommunicationManager
                        .getReportCommunication(nowJobContainerCommunication, lastJobContainerCommunication, totalTasks);
                frameworkCollector.report(reportCommunication);
                errorLimit.checkRecordLimit(reportCommunication);

                if (taskGroupContainerExecutorService.isTerminated()
                        && !hasTaskGroupException(reportCommunication)) {
                    // 结束前还需统计一次，准确统计
                    nowJobContainerCommunication = frameworkCollector.collect();
                    nowJobContainerCommunication.setTimestamp(System.currentTimeMillis());
                    reportCommunication = CommunicationManager
                            .getReportCommunication(nowJobContainerCommunication, lastJobContainerCommunication, totalTasks);
                    frameworkCollector.report(reportCommunication);
                    LOG.info("Scheduler accomplished all jobs.");
                    break;
                }

                lastJobContainerCommunication = nowJobContainerCommunication;
                Thread.sleep(jobReportIntervalInMillSec);
            } while (true);
        } catch (InterruptedException e) {
            LOG.error("捕获到InterruptedException异常!", e);
            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }

    private TaskGroupContainerRunner newTaskGroupContainerRunner(
            Configuration configuration) {
        TaskGroupContainer taskGroupContainer = ClassUtil.instantiate(
                configuration.getString(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CLASS),
                TaskGroupContainer.class, configuration);

        return new TaskGroupContainerRunner(taskGroupContainer);
    }

    private boolean hasTaskGroupException(Communication communication) {
        if(!communication.getState().equals(State.SUCCESS)) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_RUNTIME_ERROR,
                    communication.getThrowable());
        }

        return false;
    }
}
