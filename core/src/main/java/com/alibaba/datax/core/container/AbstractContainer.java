package com.alibaba.datax.core.container;

import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import org.apache.commons.lang.Validate;

import com.alibaba.datax.common.util.Configuration;

/**
 * Created by jingxing on 14-8-29.
 * 
 * 执行容器的抽象类，持有该容器全局的配置Configuration
 */
public abstract class AbstractContainer implements Container {
	protected Configuration configuration;

	protected ContainerCollector containerCollector;

	public AbstractContainer(Configuration configuration) {
		Validate.notNull(configuration, "Configuration can not be null.");

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
