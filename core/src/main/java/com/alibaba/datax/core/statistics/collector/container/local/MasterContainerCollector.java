package com.alibaba.datax.core.statistics.collector.container.local;

import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.statistics.communication.LocalSlaveContainerCommunication;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.State;
import com.alibaba.datax.common.util.Configuration;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MasterContainerCollector extends AbstractContainerCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(MasterContainerCollector.class);

    @SuppressWarnings("unused")
    private long masterContainerId;

    private String clusterManagerAddress;

    private int clusterManagerTimeout;

    public MasterContainerCollector(Configuration configuration) {
        super(configuration);
        this.masterContainerId = configuration
                .getLong(CoreConstant.DATAX_CORE_CONTAINER_MASTER_ID);
        this.clusterManagerAddress = configuration
                .getString(CoreConstant.DATAX_CORE_CLUSTERMANAGER_ADDRESS);
        this.clusterManagerTimeout = configuration
                .getInt(CoreConstant.DATAX_CORE_CLUSTERMANAGER_TIMEOUT,
                        3000);
        Validate.isTrue(StringUtils.isNotBlank(this.clusterManagerAddress),
                "在[local container collector]模式下，master的汇报地址不能为空");
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
        String message = CommunicationManager.Jsonify.getSnapshot(communication);
        LOG.info(CommunicationManager.Stringify.getSnapshot(communication));

        try {
            String result = Request.Put(String.format("%s/job/%d/status", this.clusterManagerAddress, masterContainerId))
                    .connectTimeout(this.clusterManagerTimeout).socketTimeout(this.clusterManagerTimeout)
                    .bodyString(message, ContentType.APPLICATION_JSON)
                    .execute().returnContent().asString();
            LOG.debug(result);
        } catch (Exception e) {
            LOG.warn("在[local container collector]模式下，master汇报出错: " + e.getMessage());
        }
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
        Validate.isTrue(slaveContainerId>=0, "注册的slaveContainerId不能小于0");

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
