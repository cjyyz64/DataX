package com.alibaba.datax.core.statistics.collector.container.distribute;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.container.AbstractTaskGroupContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributeTaskGroupContainerCollector extends AbstractTaskGroupContainerCollector {

    private static final Logger LOG = LoggerFactory
            .getLogger(DistributeTaskGroupContainerCollector.class);

    public DistributeTaskGroupContainerCollector(Configuration configuration) {
        super(configuration);
    }


    @Override
    public void report(Communication communication) {
        /**
         * URL：PUT /inner/job/{jobId}/taskGroup/{taskGroupId}/status
         * TODO 汇报给 DataX service
         *
         *
         */
    }

}
