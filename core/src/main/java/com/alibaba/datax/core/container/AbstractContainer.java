package com.alibaba.datax.core.container;

import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.DataxServiceUtil;
import org.apache.commons.lang.Validate;

import com.alibaba.datax.common.util.Configuration;

/**
 * Created by jingxing on 14-8-29.
 * <p/>
 * 执行容器的抽象类，持有该容器全局的配置Configuration
 */
public abstract class AbstractContainer implements Container {
    protected Configuration configuration;

    protected ContainerCollector containerCollector;

    public AbstractContainer(Configuration configuration) {
        Validate.notNull(configuration, "Configuration can not be null.");

        String dataxService = configuration.getString(
                CoreConstant.DATAX_CORE_DATAXSERVICE_ADDRESS);
        int httpTimeOut = configuration.getInt(
                CoreConstant.DATAX_CORE_DATAXSERVICE_TIMEOUT, 5000);

        DataxServiceUtil.setBasicUrl(dataxService);
        DataxServiceUtil.setTimeoutInMilliSeconds(httpTimeOut);

        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public ContainerCollector getContainerCollector() {
        return containerCollector;
    }

    public void setContainerCollector(ContainerCollector containerCollector) {
        this.containerCollector = containerCollector;
    }

}
