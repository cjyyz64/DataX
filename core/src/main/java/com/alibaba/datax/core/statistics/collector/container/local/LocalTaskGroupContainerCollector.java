package com.alibaba.datax.core.statistics.collector.container.local;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.AbstractTaskGroupContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTaskGroupCommunicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 和 StandaloneTaskGroupContainerCollector 实现一样
 */
public class LocalTaskGroupContainerCollector extends AbstractTaskGroupContainerCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(LocalTaskGroupContainerCollector.class);

    public LocalTaskGroupContainerCollector(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void report(Communication communication) {
        LOG.debug("taskGroup collector: \n" + communication.toString());

        LocalTaskGroupCommunicationManager.updateTaskGroupCommunication(
                this.taskGroupId, communication);
    }


}
