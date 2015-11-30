package com.alibaba.datax.core.statistics.container.communicator.taskgroup;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.report.DsReporter;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributeTGContainerCommunicator extends AbstractTGContainerCommunicator {

    private static final Logger LOG = LoggerFactory
            .getLogger(DistributeTGContainerCommunicator.class);

    public DistributeTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        super.setReporter(new DsReporter(super.jobId));
    }

    /**
     * 注意：这里的 report，是用于 每一个 taskGroup 搜集自身对应的 task 的状态，然后汇报到 DataxService.
     */
    @Override
    public void report(Communication communication) {
        super.getReporter().reportTGCommunication(super.taskGroupId, communication);

        LOG.info("TaskGroup => "+CommunicationTool.Stringify.getSnapshot(communication));
        reportVmInfo();
    }
}
