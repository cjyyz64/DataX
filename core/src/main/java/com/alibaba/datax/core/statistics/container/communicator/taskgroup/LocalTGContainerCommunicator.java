package com.alibaba.datax.core.statistics.container.communicator.taskgroup;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.report.ProcessInnerReporter;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.core.util.communication.CommunicationManager;
import com.alibaba.datax.core.util.communication.TGCommunicationMapHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalTGContainerCommunicator extends AbstractTGContainerCommunicator {

    private static final Logger LOG = LoggerFactory
            .getLogger(LocalTGContainerCommunicator.class);

    public LocalTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        super.setReporter(new ProcessInnerReporter());
    }

    @Override
    public void report(Communication communication) {
        TGCommunicationMapHolder.updateTaskGroupCommunication(super.taskGroupId, communication);

        LOG.info(CommunicationManager.Stringify.getSnapshot(communication));
    }
}
