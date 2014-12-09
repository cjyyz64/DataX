package com.alibaba.datax.core.statistics.collector.container;

import com.alibaba.datax.common.util.Configuration;

public abstract class AbstractContainerCollector implements ContainerCollector {
    private Configuration configuration;

    public Configuration getConfiguration() {
        return this.configuration;
    }


    public AbstractContainerCollector(Configuration configuration) {
        this.configuration = configuration;

//        Validate.isTrue(StringUtils.isNotBlank(this.dataXServiceAddress),
//                "在[local container collector]模式下，job的汇报地址不能为空");
    }
}
