package com.alibaba.datax.core.statistics.collector.container.standalone;

import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.statistics.communication.LocalSlaveContainerCommunication;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.State;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MasterContainerCollector extends AbstractContainerCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(MasterContainerCollector.class);

    public MasterContainerCollector(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        for(Configuration config : configurationList) {
            int slaveContainerId = config.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID);
            LocalSlaveContainerCommunication
                    .registerSlaveContainerCommunication(slaveContainerId, new Communication());
        }
    }

    @Override
    public void report(Communication communication) {
        LOG.debug(communication.toString());

        LOG.info(CommunicationManager.Stringify.getSnapshot(communication));
    }

    @Override
    public Communication collect() {
        return LocalSlaveContainerCommunication.getMasterCommunication();
    }

    @Override
    public State collectState() {
        return this.collect().getState();
    }

    @Override
    public Communication getCommunication(int slaveContainerId) {
        Validate.isTrue(slaveContainerId >= 0, "注册的slaveContainerId不能小于0");

        return LocalSlaveContainerCommunication
                .getSlaveContainerCommunication(slaveContainerId);
    }

    @Override
    public List<Communication> getCommunications(List<Integer> slaveContainerIds) {
        Validate.notNull(slaveContainerIds, "传入的slaveContainerIds不能为null");

        List retList = new ArrayList();
        for(int slaveContainerId : slaveContainerIds) {
            Communication communication = LocalSlaveContainerCommunication
                    .getSlaveContainerCommunication(slaveContainerId);
            if(communication!=null) {
                retList.add(communication);
            }
        }

        return retList;
    }

    @Override
    public Map<Integer, Communication> getCommunicationsMap() {
        return LocalSlaveContainerCommunication
                .getSlaveContainerCommunicationMap();
    }

}
