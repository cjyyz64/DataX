package com.alibaba.datax.core.statistics.collector.container.standalone;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.AbstractTaskGroupContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTaskGroupCommunication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskGroupContainerCollector extends AbstractTaskGroupContainerCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(TaskGroupContainerCollector.class);

    public TaskGroupContainerCollector(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void report(Communication communication) {
        LOG.debug("taskGroupContainer collector: \n" + communication.toString());

        LocalTaskGroupCommunication.updateTaskGroupCommunication(
                this.taskGroupId, communication);
    }

}
