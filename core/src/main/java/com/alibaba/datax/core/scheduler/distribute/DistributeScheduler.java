package com.alibaba.datax.core.scheduler.distribute;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scheduler.AbstractScheduler;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.service.face.domain.State;
import com.alibaba.datax.service.face.domain.TaskGroup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DistributeScheduler extends AbstractScheduler {

    @Override
    protected void startAllTaskGroup(List<Configuration> configurations) {
        //TODO 向 DataX Service 提交任务， one by one
        // TODO 转换 configuration 为 taskGroup 重试

        for (Configuration taskGroupConfig : configurations) {
            TaskGroup taskGroup = new TaskGroup();
            taskGroup.setJobId(super.getJobId());
            taskGroup.setTaskGroupId(taskGroupConfig.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID));
            taskGroup.setContext(taskGroupConfig.toJSON());
            DataxServiceUtil.startTaskGroup(super.getJobId(), taskGroup);
        }
    }

    @Override
    protected void dealFailedStat(ContainerCollector frameworkCollector, Throwable throwable) {
        long beginTime = System.currentTimeMillis();
        long maxKillTime = TimeUnit.HOURS.toMillis(8);

        Map<Integer, State> taskGroupCurrentStateMap = new HashMap<Integer, State>();

        boolean allTaskGroupFinished = true;
        while (true) {
            Communication nowJobContainerCommunication = frameworkCollector.collect();
            if (nowJobContainerCommunication.getState() == State.FAILED) {
                Map<Integer, Communication> taskGroupInJob = frameworkCollector.getCommunicationsMap();
                for (Map.Entry<Integer, Communication> entry : taskGroupInJob.entrySet()) {
                    State taskGroupState = entry.getValue().getState();
                    Integer taskGroupId = entry.getKey();
                    taskGroupCurrentStateMap.put(taskGroupId, taskGroupState);

                    if (taskGroupState.isRunning()) {
                        DataxServiceUtil.killTaskGroup(super.getJobId(), taskGroupId);
                    }
                }
            }

            for (Map.Entry<Integer, State> entry : taskGroupCurrentStateMap.entrySet()) {
                State taskGroupState = entry.getValue();
                if (!taskGroupState.isFinished()) {
                    allTaskGroupFinished = false;
                }
            }
            if (allTaskGroupFinished) {
                break;
            }

            if (System.currentTimeMillis() - beginTime >= maxKillTime) {
                throw DataXException.asDataXException(FrameworkErrorCode.KILL_JOB_TIMEOUT_ERROR,
                        "内部运行失败，在 Kill 其他运行实例时出现超时错误，需要 PE 介入处理");
            }
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException unused) {
            }
        }

        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
                throwable);

    }

    @Override
    protected void dealKillingStat(ContainerCollector frameworkCollector, int totalTasks) {
        Map<Integer, Communication> taskGroupInJob = frameworkCollector.getCommunicationsMap();

        for (Map.Entry<Integer, Communication> entry : taskGroupInJob.entrySet()) {
            if (entry.getValue().getState().isRunning()) {
                DataxServiceUtil.killTaskGroup(super.getJobId(), entry.getKey());
            }
        }

        // 认为一定是 killed 或者 failed
        boolean isAllTaskGroupFinished = true;
        for (Communication communication : taskGroupInJob.values()) {
            if (communication.getState().isRunning()) {
                isAllTaskGroupFinished = false;
                break;
            }
        }

        if (isAllTaskGroupFinished) {
            throw DataXException.asDataXException(FrameworkErrorCode.KILLED_EXIT_VALUE,
                    "DataX 被 Kill 了");
        }

    }

}
