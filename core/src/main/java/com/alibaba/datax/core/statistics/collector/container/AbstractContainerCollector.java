package com.alibaba.datax.core.statistics.collector.container;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.CoreConstant;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;

public abstract class AbstractContainerCollector implements ContainerCollector {
    private Configuration configuration;
    private String dataXServiceAddress;

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public String getDataXServiceAddress() {
        return this.dataXServiceAddress;
    }

    public AbstractContainerCollector(Configuration configuration) {
        this.configuration = configuration;

        this.dataXServiceAddress = configuration.getString(
                CoreConstant.DATAX_CORE_DATAXSERVICE_ADDRESS);
        Validate.isTrue(StringUtils.isNotBlank(this.dataXServiceAddress),
                "在[local container collector]模式下，job的汇报地址不能为空");
    }
}
