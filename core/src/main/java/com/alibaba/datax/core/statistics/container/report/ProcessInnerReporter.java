package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.util.communication.Communication;

public class ProcessInnerReporter extends AbstractReporter {
//    private static final Logger LOG = LoggerFactory
//            .getLogger(ProcessInnerReporter.class);

    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {
//        LOG.info(CommunicationManager.Stringify.getSnapshot(communication));
    }
}