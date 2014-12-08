package com.alibaba.datax.core.scheduler.distribute;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scheduler.AbstractScheduler;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;

import java.util.List;

public class DistributeScheduler extends AbstractScheduler {

    @Override
    protected void startAllTaskGroup(List<Configuration> configurations) {
        //TODO 向 DataX Service 提交任务， one by one
    }

    @Override
    protected void checkAndDealFailedStat(ContainerCollector frameworkCollector, Communication nowJobContainerCommunication, int totalTasks) {
        // TODO 检查到 job 失败，还需要跟 DS 请求 kill 其他 tg
    }

    @Override
    protected boolean checkAndDealSucceedStat(ContainerCollector frameworkCollector, Communication lastJobContainerCommunication, int totalTasks) {
        return false;
    }

    @Override
    protected void checkAndDealKillingStat(ContainerCollector frameworkCollector, int totalTasks) {

    }
}
