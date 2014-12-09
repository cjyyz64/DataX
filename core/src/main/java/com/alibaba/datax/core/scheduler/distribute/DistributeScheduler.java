package com.alibaba.datax.core.scheduler.distribute;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scheduler.AbstractScheduler;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.State;
import com.alibaba.datax.service.face.domain.Result;
import com.alibaba.datax.service.face.domain.TaskGroup;

import java.util.List;

public class DistributeScheduler extends AbstractScheduler {

    @Override
    protected void startAllTaskGroup(List<Configuration> configurations) {
        //TODO 向 DataX Service 提交任务， one by one

        // TODO 转换 configuration 为 taskGroup 重试
        TaskGroup taskGroup = null;
        for (Configuration taskGroupConfig : configurations) {
            DataxServiceUtil.startTaskGroup(super.getJobId(), taskGroup);
        }
    }

    @Override
    protected void checkAndDealFailedStat(ContainerCollector frameworkCollector, Communication nowJobContainerCommunication, int totalTasks) {
        // TODO 检查到 job 失败，还需要跟 DS 请求 kill 其他 tg
        if (nowJobContainerCommunication.getState() == State.FAIL) {
            Result<List<TaskGroup>> taskGroupInJob = DataxServiceUtil.getTaskGroupInJob(super.getJobId());
            for (TaskGroup taskGroup : taskGroupInJob.getData()) {
                if (taskGroup.getState().isRunning()) {
                    DataxServiceUtil.killTaskGroup(super.getJobId(), taskGroup.getTaskGroupId());
                }
            }
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
                    nowJobContainerCommunication.getThrowable());
        }

    }

    @Override
    protected boolean checkAndDealSucceedStat(ContainerCollector frameworkCollector, Communication lastJobContainerCommunication, int totalTasks) {
        Communication nowJobContainerCommunication = frameworkCollector.collect();
        if (nowJobContainerCommunication.getState().isSucceed()) {
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
        Result<?> jobInfo = DataxServiceUtil.getJobInfo(super.getJobId());
        jobInfo.getData();

        //如果job 的状态是 killing，则 去杀 tg 最后 再以 143 退出
    }
}
