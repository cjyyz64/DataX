package com.alibaba.datax.core;

import com.alibaba.datax.core.statistics.container.ContainerCommunicator;
import com.alibaba.datax.core.common.CoreConstant;
import com.alibaba.datax.core.util.DataxServiceUtil;
import org.apache.commons.lang.Validate;

import com.alibaba.datax.common.util.Configuration;

/**
 * 执行容器的抽象类，持有该容器全局的配置 configuration
 */
public abstract class AbstractContainer {
    protected Configuration configuration;

    protected ContainerCommunicator containerCommunicator;

    public AbstractContainer(Configuration configuration) {
        Validate.notNull(configuration, "Configuration can not be null.");

        String dataxServiceUrl = configuration.getString(
                CoreConstant.DATAX_CORE_DATAXSERVICE_ADDRESS);
        int httpTimeOutInMillionSeconds = configuration.getInt(
                CoreConstant.DATAX_CORE_DATAXSERVICE_TIMEOUT, 5000);

        DataxServiceUtil.init(dataxServiceUrl, httpTimeOutInMillionSeconds);

        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public ContainerCommunicator getContainerCommunicator() {
        return containerCommunicator;
    }

    public void setContainerCommunicator(ContainerCommunicator containerCommunicator) {
        this.containerCommunicator = containerCommunicator;
    }

    public abstract void start();

}
