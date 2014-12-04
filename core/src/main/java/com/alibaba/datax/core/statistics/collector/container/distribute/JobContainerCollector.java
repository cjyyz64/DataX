package com.alibaba.datax.core.statistics.collector.container.distribute;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.util.State;

import java.util.List;
import java.util.Map;

public class JobContainerCollector extends AbstractContainerCollector {

    public JobContainerCollector(Configuration configuration) {
        super(configuration);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        // TODO Auto-generated method stub
    }

    @Override
    public void report(Communication communication) {
        // TODO Auto-generated method stub
    }

    @Override
    public Communication collect() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public State collectState() {
        return null;
    }

    @Override
    public Communication getCommunication(int taskGroupId) {
        return null;
    }

    @Override
    public List<Communication> getCommunications(List<Integer> taskGroupIds) {
        return null;
    }

    @Override
    public Map<Integer, Communication> getCommunicationsMap() {
        return null;
    }

}
