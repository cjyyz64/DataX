package com.alibaba.datax.core.statistics.collector.container.standalone;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.AbstractTaskGroupContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTaskGroupCommunicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandaloneTaskGroupContainerCollector extends AbstractTaskGroupContainerCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(StandaloneTaskGroupContainerCollector.class);

    public StandaloneTaskGroupContainerCollector(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void report(Communication communication) {
        LOG.debug("taskGroupContainer collector: \n" + communication.toString());

        LocalTaskGroupCommunicationManager.updateTaskGroupCommunication(
                this.taskGroupId, communication);
    }

}
