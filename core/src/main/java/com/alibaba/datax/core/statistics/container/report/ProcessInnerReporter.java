package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.core.util.communication.TGCommunicationMapHolder;

public class ProcessInnerReporter extends AbstractReporter {
//    private static final Logger LOG = LoggerFactory
//            .getLogger(ProcessInnerReporter.class);

    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {
//        LOG.info(CommunicationManager.Stringify.getSnapshot(communication));
    }

    @Override
    public void reportTGCommunication(Integer taskGroupId, Communication communication) {
        TGCommunicationMapHolder.updateTaskGroupCommunication(taskGroupId, communication);
    }
}