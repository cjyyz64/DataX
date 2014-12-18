package com.alibaba.datax.core.statistics.container.communicator.taskgroup;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.collector.ProcessInnerCollector;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.dataxservice.face.domain.State;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 该类是用于处理 taskGroupContainer 的 communication 的收集汇报的父类
 * 主要是 taskCommunicationMap 记录了 taskExecutor 的 communication 属性
 */
public class AbstractTGContainerCommunicator extends AbstractContainerCommunicator {
    private Map<Integer, Communication> taskCommunicationMap;

    /**
     * 由于taskGroupContainer是进程内部调度
     * 其registerCommunication()，getCommunication()，
     * getCommunications()，collect()等方法是一致的
     * 所有TG的Collector都是ProcessInnerCollector
     */
    protected int taskGroupId;

    public AbstractTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        super.setCollector(new ProcessInnerCollector());
        this.taskGroupId = configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        this.taskCommunicationMap = super.getCollector().registerTaskCommunication(configurationList);
    }

    @Override
    public final Communication getCommunication(int taskId) {
        Validate.isTrue(taskId >= 0, "注册的taskId不能小于0");

        return this.taskCommunicationMap.get(taskId);
    }

    @Override
    public final Communication collect() {
        return this.getCollector().collectFromTask();
    }

    @Override
    public final State collectState() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskCommunication :
                this.taskCommunicationMap.values()) {
            communication.mergeStateFrom(taskCommunication);
        }

        return communication.getState();
    }

    @Override
    public final List<Communication> getCommunications(List<Integer> taskIds) {
        Validate.notNull(taskIds, "传入的taskIds不能为null");

        List retList = new ArrayList();
        for (int taskId : taskIds) {
            Validate.isTrue(taskId >= 0, "注册的taskId不能小于0");
            Communication communication = this.taskCommunicationMap
                    .get(taskId);
            if (null != communication) {
                retList.add(communication);
            }
        }

        return retList;
    }

    @Override
    public void report(Communication communication) {
        super.getReporter().updateTGCommication(this.taskGroupId, communication);
    }

    @Override
    public final Map<Integer, Communication> getCommunicationsMap() {
        return taskCommunicationMap;
    }

}
