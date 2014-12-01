package com.alibaba.datax.core.statistics.collector.container.local;

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

    /**
     * TODO 目前由于分布式还没有上去，所以先将local的slave report设置为跟standalone一样，
     * 实际应该反馈给上端http去更新，等分布式上了再改
     *
     * @param communication
     */
    @Override
    public void report(Communication communication) {
        LOG.debug("slaveContainer collector: \n" + communication.toString());
        LocalSlaveContainerCommunication.updateSlaveContainerCommunication(
                this.slaveContainerId, communication);
    }
}
