package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;

public class ProcessInnerReporter extends AbstractReporter {
//    private static final Logger LOG = LoggerFactory
//            .getLogger(ProcessInnerReporter.class);

    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {
//        LOG.info(CommunicationManager.Stringify.getSnapshot(communication));
    }

    @Override
    public void reportTGCommunication(Integer taskGroupId, Communication communication) {
        LocalTGCommunicationManager.updateTaskGroupCommunication(taskGroupId, communication);
    }
}