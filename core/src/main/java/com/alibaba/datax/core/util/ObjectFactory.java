package com.alibaba.datax.core.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.collector.AbstractCollector;
import com.alibaba.datax.core.statistics.container.report.AbstractReporter;


//TODO
public final class ObjectFactory {

    public static AbstractContainerCommunicator createAContainerCollector(Configuration configuration) {
        //对 configuration 进行判断，进而得出其运行模式，再创建合适对象

        return null;
    }

    public static AbstractReporter createAReporter(Configuration configuration) {
        //对 configuration 进行判断，进而得出其运行模式，再创建合适对象

        return null;
    }

    public static AbstractCollector createACollector(Configuration configuration) {
        //对 configuration 进行判断，进而得出其运行模式，再创建合适对象

        return null;
    }
}
