package com.alibaba.datax.core.scheduler;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.TaskGroupContainer;
import com.alibaba.datax.core.scheduler.standalone.TaskGroupContainerRunner;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AbstractScheduler implements Scheduler {
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

        while(true){
            // 分布式情况下，需要先查询job下属taskGroup的状态，再合并为 JobContainer 的状态
            Communication nowJobContainerCommunication = frameworkCollector.collect();
            nowJobContainerCommunication.setTimestamp(System.currentTimeMillis());
            if(nowJobContainerCommunication.getState().isFailed()){
                taskGroupContainerExecutorService.shutdownNow();
                throw DataXException.asDataXException(
                        FrameworkErrorCode.PLUGIN_RUNTIME_ERROR,
                        nowJobContainerCommunication.getThrowable());
            }else if(nowJobContainerCommunication.getState().isSucceed()){
                Communication reportCommunication = CommunicationManager
                        .getReportCommunication(nowJobContainerCommunication, lastJobContainerCommunication, totalTasks);
                frameworkCollector.report(reportCommunication);
                errorLimit.checkRecordLimit(reportCommunication);
                break;
            }

            boolean isKilling = checkIfKilling();
            if(isKilling){


            }




        }

    }

    protected boolean checkIfKilling(){
        //去 DS 根据 jobId 查询其状态，URL:GET /job/{jobId}/status
    }


    private TaskGroupContainerRunner newTaskGroupContainerRunner(
            Configuration configuration) {
        TaskGroupContainer taskGroupContainer = ClassUtil.instantiate(
                configuration.getString(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CLASS),
                TaskGroupContainer.class, configuration);

        return new TaskGroupContainerRunner(taskGroupContainer);
    }

}
