package com.alibaba.datax.core.statistics.container.communicator.taskgroup;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.report.ProcessInnerReporter;

/**
 * Date: 2014/12/18 11:12
 *
 * @author tianjin.lp <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class StandaloneTGContainerCommunicator extends AbstractTGContainerCommunicator {

    public StandaloneTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        super.setReporter(new ProcessInnerReporter());
    }

}
