package com.alibaba.datax.core.scheduler.local;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.TaskGroupContainer;
import com.alibaba.datax.core.scheduler.AbstractScheduler;
import com.alibaba.datax.core.scheduler.standalone.TaskGroupContainerRunner;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.State;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LocalScheduler extends AbstractScheduler {

    private ExecutorService taskGroupContainerExecutorService;
    private List<TaskGroupContainerRunner> taskGroupContainerRunners = new ArrayList<TaskGroupContainerRunner>();


    @Override
    protected boolean startAllTaskGroup(List<Configuration> configurations) {
        this.taskGroupContainerExecutorService = Executors
                .newFixedThreadPool(configurations.size());

        for (Configuration taskGroupConfiguration : configurations) {
            TaskGroupContainerRunner taskGroupContainerRunner = newTaskGroupContainerRunner(taskGroupConfiguration);
            taskGroupContainerExecutorService.execute(taskGroupContainerRunner);
            taskGroupContainerRunners.add(taskGroupContainerRunner);
        }
        taskGroupContainerExecutorService.shutdown();

        return true;
    }

    @Override
    protected void checkAndDealFailedStat(ContainerCollector frameworkCollector, Communication nowJobContainerCommunication, int totalTasks) {
        if (nowJobContainerCommunication.getState() == State.FAIL) {
            taskGroupContainerExecutorService.shutdownNow();
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_RUNTIME_ERROR,
                    nowJobContainerCommunication.getThrowable());
        }
    }

    @Override
    protected boolean checkAndDealSucceedStat(ContainerCollector frameworkCollector, Communication lastJobContainerCommunication, int totalTasks) {
        Communication nowJobContainerCommunication = null;
        if (taskGroupContainerExecutorService.isTerminated()) {
            // 结束前还需统计一次，准确统计
            nowJobContainerCommunication = frameworkCollector.collect();
            nowJobContainerCommunication.setTimestamp(System.currentTimeMillis());
            Communication reportCommunication = CommunicationManager
                    .getReportCommunication(nowJobContainerCommunication, lastJobContainerCommunication, totalTasks);
            frameworkCollector.report(reportCommunication);

            return true;
        }

        return false;
    }

    @Override
    protected void checkAndDealKillingStat(ContainerCollector frameworkCollector, int totalTasks) {
        //通过进程退出返回码标示状态
    }

    private TaskGroupContainerRunner newTaskGroupContainerRunner(
            Configuration configuration) {
        TaskGroupContainer taskGroupContainer = ClassUtil.instantiate(
                configuration.getString(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CLASS),
                TaskGroupContainer.class, configuration);

        return new TaskGroupContainerRunner(taskGroupContainer);
    }

}
