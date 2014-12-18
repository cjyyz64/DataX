package com.alibaba.datax.core.statistics.container.collector.job;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.container.collector.AbstractCollector;
import com.alibaba.datax.core.statistics.container.report.AbstractReporter;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.core.util.communication.CommunicationManager;
import com.alibaba.datax.core.util.communication.LocalTaskGroupCommunicationManager;
import com.alibaba.datax.dataxservice.face.domain.State;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LocalJobContainerCollecor extends AbstractContainerCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(LocalJobContainerCollecor.class);

    public LocalJobContainerCollecor(Configuration configuration, AbstractCollector collector, AbstractReporter reporter) {
        super(configuration, collector, reporter);
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
    public State collectState() {
        return this.collect().getState();
    }

    /**
     * 和 DistributeJobContainerCollector 的 report 实现一样
     */
    @Override
    public void report(Communication communication) {
        super.getReporter().updateJobCommunication(super.getJobId(), communication);

        LOG.info(CommunicationManager.Stringify.getSnapshot(communication));
    }

    @Override
    public Communication getCommunication(int taskGroupId) {
        Validate.isTrue(taskGroupId >= 0, "注册的taskGroupId不能小于0");

        return LocalTaskGroupCommunicationManager
                .getTaskGroupCommunication(taskGroupId);
    }

    @Override
    public List<Communication> getCommunications(List<Integer> taskGroupIds) {
        Validate.notNull(taskGroupIds, "传入的taskGroupIds不能为null");

        List retList = new ArrayList();
        for (int taskGroupId : taskGroupIds) {
            Communication communication = LocalTaskGroupCommunicationManager
                    .getTaskGroupCommunication(taskGroupId);
            if (communication != null) {
                retList.add(communication);
            }
        }

        return retList;
    }

    @Override
    public Map<Integer, Communication> getCommunicationsMap() {
        return LocalTaskGroupCommunicationManager
                .getTaskGroupCommunicationMap();
    }

}
