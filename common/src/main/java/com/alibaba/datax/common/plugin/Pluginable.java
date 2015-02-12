package com.alibaba.datax.common.plugin;

import com.alibaba.datax.common.util.Configuration;

public interface Pluginable {
	String getName();

	String getDeveloper();

    String getDescription();

	void init();

	void destroy();

    Configuration getPluginJobConf();

    Configuration getWriterConf();

    Configuration getReaderConf();

    void setPluginConf(Configuration pluginConf);

	void setPluginJobConf(Configuration jobConf);

    void setReaderConf(Configuration readerConf);

    void setWriterConf(Configuration writerConf);

    public String getReaderPluginName();

    public void setReaderPluginName(String readerPluginName);

    public String getWriterPluginName();

    public void setWriterPluginName(String writerPluginName);

}
