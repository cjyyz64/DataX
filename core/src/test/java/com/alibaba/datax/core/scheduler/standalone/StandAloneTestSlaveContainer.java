package com.alibaba.datax.core.scheduler.standalone;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.AbstractContainer;

/**
 * Created by jingxing on 14-9-4.
 */
public class StandAloneTestSlaveContainer extends AbstractContainer {
    public StandAloneTestSlaveContainer(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void start() {
        System.out.println("start standAlone test slave container");
    }
}
