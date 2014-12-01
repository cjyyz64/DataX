package com.alibaba.datax.core.statistics.collector.container.standalone;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.AbstractSlaveContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalSlaveContainerCommunication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlaveContainerCollector extends AbstractSlaveContainerCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(SlaveContainerCollector.class);

    public SlaveContainerCollector(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void report(Communication communication) {
        LOG.debug("slaveContainer collector: \n" + communication.toString());
        LocalSlaveContainerCommunication.updateSlaveContainerCommunication(
                this.slaveContainerId, communication);
    }

}
