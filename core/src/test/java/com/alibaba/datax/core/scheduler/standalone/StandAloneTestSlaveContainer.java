package com.alibaba.datax.core.scheduler.standalone;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.SlaveContainer;

/**
 * Created by jingxing on 14-9-4.
 */
public class StandAloneTestSlaveContainer extends SlaveContainer {
    public StandAloneTestSlaveContainer(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void start() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("start standAlone test slave container");
    }
}
