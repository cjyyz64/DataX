package com.alibaba.datax.common.plugin;

import com.alibaba.datax.common.util.Configuration;

public interface Pluginable {
	String getName();

	String getDeveloper();

    String getDescription();

	void init();

	void destroy();

    Configuration getPluginJobConf();

    void setPluginConf(Configuration pluginConf);

	void setPluginJobConf(Configuration jobConf);

}
