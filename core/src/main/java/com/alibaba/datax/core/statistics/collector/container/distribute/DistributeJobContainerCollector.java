package com.alibaba.datax.core.statistics.collector.container.distribute;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class DistributeJobContainerCollector extends AbstractContainerCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(DistributeJobContainerCollector.class);

    private long jobId;

    // TODO delete it ?   unused
    private int dataXServiceTimeout;


    public DistributeJobContainerCollector(Configuration configuration) {
        super(configuration);
        this.jobId = configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        this.dataXServiceTimeout = configuration.getInt(
                CoreConstant.DATAX_CORE_DATAXSERVICE_TIMEOUT, 3000);
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        // do nothing
    }

    //TODO  参考 LocalJobContainerCollector 的 repot 的实现
    @Override
    public void report(Communication communication) {
        String message = CommunicationManager.Jsonify.getSnapshot(communication);
        // TODO
        // 1、 把 json 格式的 message 封装为 t_job表对应的 Data Object
        // 2、 把 统计的状态，汇报给 DS

        // 注意：汇报将会重试，如果汇报失败，打印 error 日志，但是继续，不退出

        try {
            LOG.debug(message);
        } catch (Exception e) {
            LOG.warn("在[local container collector]模式下，job汇报出错: " + e.getMessage());
        }
    }

    @Override
    public Communication collect() {
        /**
         *
         *
         *查询 job 下属 taskGroup 的状态，再合并其状态，作为 jobContainer 的状态
         1、 查询job下属taskGroup  URL：GET /job/{jobId}/taskGroup
         *
         * Communication communication = new Communication();
         communication.setState(State.SUCCESS);

         for(Communication taskGroupCommunication :
         taskGroupCommunicationMap.values()) {
         communication.mergeFrom(taskGroupCommunication);
         }

         return communication;
         *
         */


        return null;
    }

    @Override
    public State collectState() {
        // 注意：这里会通过 this.collect() 再走一次网络
        return this.collect().getState();
    }

    @Override
    public Communication getCommunication(int taskGroupId) {
        /**
         * URL：GET /job/{jobId}/taskGroup/{taskGroupId}
         *
         * 查看对应 taskGroup 的详情，转换为 Communication 即可
         */
        return null;
    }

    @Override
    public List<Communication> getCommunications(List<Integer> taskGroupIds) {
        // TODO 暂时没有地方使用 skip it
        return null;
    }

    @Override
    public Map<Integer, Communication> getCommunicationsMap() {
        // TODO 暂时没有地方使用 skip it
        return null;
    }

}
