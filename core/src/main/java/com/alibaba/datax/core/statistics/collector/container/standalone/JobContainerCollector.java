package com.alibaba.datax.core.statistics.collector.container.standalone;

import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.statistics.communication.LocalTaskGroupCommunication;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.State;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JobContainerCollector extends AbstractContainerCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(JobContainerCollector.class);

    public JobContainerCollector(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        for(Configuration config : configurationList) {
            int taskGroupId = config.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            LocalTaskGroupCommunication
                    .registerTaskGroupCommunication(taskGroupId, new Communication());
        }
    }

    @Override
    public void report(Communication communication) {
        LOG.debug(communication.toString());

        LOG.info(CommunicationManager.Stringify.getSnapshot(communication));
    }

    @Override
    public Communication collect() {
        return LocalTaskGroupCommunication.getJobCommunication();
    }

    @Override
    public State collectState() {
        return this.collect().getState();
    }

    @Override
    public Communication getCommunication(int taskGroupId) {
        Validate.isTrue(taskGroupId >= 0, "注册的taskGroupId不能小于0");

        return LocalTaskGroupCommunication
                .getTaskGroupCommunication(taskGroupId);
    }

    @Override
    public List<Communication> getCommunications(List<Integer> taskGroupIds) {
        Validate.notNull(taskGroupIds, "传入的taskGroupIds不能为null");

        List retList = new ArrayList();
        for(int taskGroupId : taskGroupIds) {
            Communication communication = LocalTaskGroupCommunication
                    .getTaskGroupCommunication(taskGroupId);
            if(communication!=null) {
                retList.add(communication);
            }
        }

        return retList;
    }

    @Override
    public Map<Integer, Communication> getCommunicationsMap() {
        return LocalTaskGroupCommunication
                .getTaskGroupCommunicationMap();
    }

}
