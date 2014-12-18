package com.alibaba.datax.core.statistics.container.communicator.job;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.collector.DsCollector;
import com.alibaba.datax.core.statistics.container.report.DsReporter;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.dataxservice.face.domain.State;

import java.util.List;
import java.util.Map;

public class DistributeJobContainerCommunicator extends AbstractContainerCommunicator {

    public DistributeJobContainerCommunicator(Configuration configuration) {
        super(configuration);
        super.setCollector(new DsCollector());
        super.setReporter(new DsReporter());
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        super.getCollector().registerTGCommunication(configurationList);
    }


    @Override
    public Communication collect() {
        return super.getCollector().collectFromTaskGroup();
    }

    @Override
    public void report(Communication communication) {
        super.getReporter().updateJobCommunication(super.getJobId(), communication);
    }


    @Override
    public State collectState() {
        // 注意：这里会通过 this.collect() 再走一次网络
        return this.collect().getState();
    }

    @Override
    public Communication getCommunication(int id) {
        return null;
    }

    @Override
    public List<Communication> getCommunications(List<Integer> ids) {
        return null;
    }

    //TODO 让 collecor 开放 其内部 Map 数据结构
    @Override
    public Map<Integer, Communication> getCommunicationsMap() {

        return null;
    }
}
