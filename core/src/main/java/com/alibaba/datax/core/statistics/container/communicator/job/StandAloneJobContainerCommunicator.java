package com.alibaba.datax.core.statistics.container.communicator.job;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.common.CoreConstant;
import com.alibaba.datax.core.statistics.container.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.collector.ProcessInnerCollector;
import com.alibaba.datax.core.statistics.container.report.ProcessInnerReporter;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.core.util.communication.TGCommunicationMapHolder;
import com.alibaba.datax.dataxservice.face.domain.State;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StandAloneJobContainerCommunicator extends AbstractContainerCommunicator {

    public StandAloneJobContainerCommunicator(Configuration configuration) {
        super(configuration);
        super.setCollector(new ProcessInnerCollector(configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID)));
        super.setReporter(new ProcessInnerReporter());
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
        super.getReporter().reportJobCommunication(super.getJobId(), communication);
    }

    @Override
    public List<Communication> getCommunications(List<Integer> taskGroupIds) {
        Validate.notNull(taskGroupIds, "传入的 taskGroupIds 不能为null");

        List retList = new ArrayList();
        for (int taskGroupId : taskGroupIds) {
            Validate.isTrue(taskGroupId >= 0, "注册的 taskGroupId 不能小于0");
            Communication communication = super.getCollector().getTaskCommunicationMap()
                    .get(taskGroupId);
            if (null != communication) {
                retList.add(communication);
            }
        }

        return retList;
    }

    @Override
    public Map<Integer, Communication> getCommunicationMap() {
        return TGCommunicationMapHolder.getTaskGroupCommunicationMap();
    }
}
