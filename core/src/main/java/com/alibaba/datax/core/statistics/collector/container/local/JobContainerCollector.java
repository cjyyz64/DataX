package com.alibaba.datax.core.statistics.collector.container.local;

import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.statistics.communication.LocalTaskGroupCommunication;
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

public class JobContainerCollector extends AbstractContainerCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(JobContainerCollector.class);

    @SuppressWarnings("unused")
    private long jobId;

    private String dataXServiceAddress;

    private int dataXServiceTimeout;

    public JobContainerCollector(Configuration configuration) {
        super(configuration);
        this.jobId = configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        this.dataXServiceAddress = configuration.getString(
                CoreConstant.DATAX_CORE_DATAXSERVICE_ADDRESS);
        this.dataXServiceTimeout = configuration.getInt(
                CoreConstant.DATAX_CORE_DATAXSERVICE_TIMEOUT, 3000);
        Validate.isTrue(StringUtils.isNotBlank(this.dataXServiceAddress),
                "在[local container collector]模式下，job的汇报地址不能为空");
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        for(Configuration config : configurationList) {
            int taskGroupId = config.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            LocalTaskGroupCommunication.registerTaskGroupCommunication(
                    taskGroupId, new Communication());
        }
    }

    @Override
    public void report(Communication communication) {
        String message = CommunicationManager.Jsonify.getSnapshot(communication);
        LOG.info(CommunicationManager.Stringify.getSnapshot(communication));

        try {
            String result = Request.Put(String.format("%s/inner/job/%d/status", this.dataXServiceAddress, jobId))
                    .connectTimeout(this.dataXServiceTimeout).socketTimeout(this.dataXServiceTimeout)
                    .bodyString(message, ContentType.APPLICATION_JSON)
                    .execute().returnContent().asString();
            LOG.debug(result);
        } catch (Exception e) {
            LOG.warn("在[local container collector]模式下，job汇报出错: " + e.getMessage());
        }
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
        Validate.isTrue(taskGroupId>=0, "注册的taskGroupId不能小于0");

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
