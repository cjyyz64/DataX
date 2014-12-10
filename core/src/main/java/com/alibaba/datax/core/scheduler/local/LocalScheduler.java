package com.alibaba.datax.core.scheduler.local;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.TaskGroupContainer;
import com.alibaba.datax.core.scheduler.AbstractScheduler;
import com.alibaba.datax.core.scheduler.standalone.TaskGroupContainerRunner;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.service.face.domain.Job;
import com.alibaba.datax.service.face.domain.Result;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LocalScheduler extends AbstractScheduler {

    private ExecutorService taskGroupContainerExecutorService;

    @Override
    protected void startAllTaskGroup(List<Configuration> configurations) {
        this.taskGroupContainerExecutorService = Executors
                .newFixedThreadPool(configurations.size());

        for (Configuration taskGroupConfiguration : configurations) {
            TaskGroupContainerRunner taskGroupContainerRunner = newTaskGroupContainerRunner(taskGroupConfiguration);
            this.taskGroupContainerExecutorService.execute(taskGroupContainerRunner);
        }

        this.taskGroupContainerExecutorService.shutdown();
    }

    @Override
    protected void dealFailedStat(ContainerCollector frameworkCollector, Throwable throwable) {
        this.taskGroupContainerExecutorService.shutdownNow();
        throw DataXException.asDataXException(
                FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
    }


    @Override
    protected void dealKillingStat(ContainerCollector frameworkCollector, int totalTasks) {
        //通过进程退出返回码标示状态
        Result<Job> jobInfo = DataxServiceUtil.getJobInfo(super.getJobId());
        com.alibaba.datax.service.face.domain.State state = jobInfo.getData().getState();
        if (state.equals(com.alibaba.datax.service.face.domain.State.KILLING)) {
            this.taskGroupContainerExecutorService.shutdownNow();
            throw DataXException.asDataXException(FrameworkErrorCode.KILLED_EXIT_VALUE,
                    "job killed status");
        }
    }

    private TaskGroupContainerRunner newTaskGroupContainerRunner(
            Configuration configuration) {
        TaskGroupContainer taskGroupContainer = ClassUtil.instantiate(
                configuration.getString(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CLASS),
                TaskGroupContainer.class, configuration);

        return new TaskGroupContainerRunner(taskGroupContainer);
    }

}
