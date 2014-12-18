package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.core.util.communication.CommunicationManager;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SingleProcessReporter extends AbstractReporter {
    private static final Logger LOG = LoggerFactory
            .getLogger(SingleProcessReporter.class);


    // how to init it ? TODO
    private Map<Integer, Communication> taskGroupCommunicationMap;

    @Override
    public void updateJobCommunication(Long jobId, Communication communication) {
        LOG.info(CommunicationManager.Stringify.getSnapshot(communication));
    }

    //也就是更新 tg 的状态
    @Override
    public void updateTGCommication(Integer taskGroupId, Communication communication) {
        Validate.isTrue(taskGroupCommunicationMap.containsKey(
                taskGroupId), String.format("taskGroupCommunicationMap中没有注册taskGroupId[%d]的Communication，" +
                "无法更新该taskGroup的信息", taskGroupId));
        taskGroupCommunicationMap.put(taskGroupId, communication);
    }

}