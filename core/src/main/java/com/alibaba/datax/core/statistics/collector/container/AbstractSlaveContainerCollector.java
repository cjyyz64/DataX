package com.alibaba.datax.core.statistics.collector.container;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.State;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jingxing on 14/11/8.
 *
 * 该类是用于处理slaveContainer的communication的收集汇报的父类
 * 主要是slaveExecutorCommunicationMap记录了slaveExecutor的communication属性
 */
public abstract class AbstractSlaveContainerCollector extends AbstractContainerCollector{
    protected Map<Integer, Communication>slaveExecutorCommunicationMap =
            new ConcurrentHashMap<Integer, Communication>();

    /**
     * 由于slaveContainer是进程内部调度
     * 其registerCommunication()，getCommunication()，
     * getCommunications()，collect()等方法是一致的
     */
    protected int slaveContainerId;

    public AbstractSlaveContainerCollector(Configuration configuration) {
        super(configuration);
        this.slaveContainerId = configuration
                .getInt(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID);
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        for (Configuration slaveExecutorConfig : configurationList) {
            int slaveExecutorId = slaveExecutorConfig
                    .getInt(CoreConstant.JOB_TASKID);
            this.slaveExecutorCommunicationMap.put(
                    slaveExecutorId, new Communication());
        }
    }

    @Override
    public final Communication getCommunication(int slaveExecutorId) {
        Validate.isTrue(slaveExecutorId >= 0, "注册的slaveExecutorId不能小于0");

        return this.slaveExecutorCommunicationMap.get(slaveExecutorId);
    }

    @Override
    public final Communication collect() {
        Communication communication = new Communication();
        communication.setState(State.SUCCESS);

        for(Communication slaveCommunication :
                this.slaveExecutorCommunicationMap.values()) {
            communication.mergeFrom(slaveCommunication);
        }

        return communication;
    }

    @Override
    public final State collectState() {
        Communication communication = new Communication();
        communication.setState(State.SUCCESS);

        for(Communication slaveCommunication :
                this.slaveExecutorCommunicationMap.values()) {
            communication.mergeStateFrom(slaveCommunication);
        }

        return communication.getState();
    }

    @Override
    public final List<Communication> getCommunications(List<Integer> slaveExecutorIds) {
        Validate.notNull(slaveExecutorIds, "传入的slaveExecutorIds不能为null");

        List retList = new ArrayList();
        for(int slaveExecutorId : slaveExecutorIds) {
            Validate.isTrue(slaveExecutorId >= 0, "注册的slaveExecutorId不能小于0");
            Communication communication = this.slaveExecutorCommunicationMap
                    .get(slaveExecutorId);
            if(null != communication) {
                retList.add(communication);
            }
        }

        return retList;
    }

    @Override
    public final Map<Integer, Communication> getCommunicationsMap() {
        return slaveExecutorCommunicationMap;
    }
}
