 package com.alibaba.datax.core.statistics.collector.container;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.State;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jingxing on 14/11/8.
 * <p/>
 * 该类是用于处理taskGroupContainer的communication的收集汇报的父类
 * 主要是taskCommunicationMap记录了taskExecutor的communication属性
 */
public abstract class AbstractTaskGroupContainerCollector extends AbstractContainerCollector {
    protected Map<Integer, Communication> taskCommunicationMap =
            new ConcurrentHashMap<Integer, Communication>();

    protected long jobId;

    /**
     * 由于taskGroupContainer是进程内部调度
     * 其registerCommunication()，getCommunication()，
     * getCommunications()，collect()等方法是一致的
     */
    protected int taskGroupId;

    public AbstractTaskGroupContainerCollector(Configuration configuration) {
        super(configuration);
        this.jobId = configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        this.taskGroupId = configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        for (Configuration taskConfig : configurationList) {
            int taskId = taskConfig.getInt(
                    CoreConstant.JOB_TASK_ID);
            this.taskCommunicationMap.put(
                    taskId, new Communication());
        }
    }

    @Override
    public final Communication getCommunication(int taskId) {
        Validate.isTrue(taskId >= 0, "注册的taskId不能小于0");

        return this.taskCommunicationMap.get(taskId);
    }

    @Override
    public final Communication collect() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskCommunication :
                this.taskCommunicationMap.values()) {
            communication.mergeFrom(taskCommunication);
        }

        return communication;
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
    public final Map<Integer, Communication> getCommunicationsMap() {
        return taskCommunicationMap;
    }
}
