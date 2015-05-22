package com.alibaba.datax.common.plugin;

import com.alibaba.datax.common.base.BaseObject;
import com.alibaba.datax.common.util.Configuration;

public abstract class AbstractPlugin extends BaseObject implements Pluginable {
	private Configuration pluginJobConf;

	private Configuration pluginConf;


    private Configuration readerConf;

    private Configuration writerConf;



    private String readerPluginName;

    private String writerPluginName;

    @Override
	public String getName() {
		assert null != this.pluginConf;
		return this.pluginConf.getString("name");
	}

    @Override
	public String getDeveloper() {
		assert null != this.pluginConf;
		return this.pluginConf.getString("developer");
	}

    @Override
	public String getDescription() {
		assert null != this.pluginConf;
		return this.pluginConf.getString("description");
	}

    @Override
	public Configuration getPluginJobConf() {
		return pluginJobConf;
	}

    @Override
	public void setPluginJobConf(Configuration pluginJobConf) {
		this.pluginJobConf = pluginJobConf;
	}

    @Override
	public void setPluginConf(Configuration pluginConf) {
		this.pluginConf = pluginConf;
	}

    @Override
    public Configuration getReaderConf() {
        return readerConf;
    }

    @Override
    public void setReaderConf(Configuration readerConf) {
        this.readerConf = readerConf;
    }

    @Override
    public Configuration getWriterConf() {
        return writerConf;
    }

    @Override
    public void setWriterConf(Configuration writerConf) {
        this.writerConf = writerConf;
    }

    @Override
    public String getReaderPluginName() {
        return readerPluginName;
    }

    @Override
    public void setReaderPluginName(String readerPluginName) {
        this.readerPluginName = readerPluginName;
    }

    @Override
    public String getWriterPluginName() {
        return writerPluginName;
    }

    @Override
    public void setWriterPluginName(String writerPluginName) {
        this.writerPluginName = writerPluginName;
    }

	public void prepare() {
	}

	public void post() {
	}

    public void preHandler(Configuration jobConfiguration){

    }

    public void postHandler(Configuration jobConfiguration){

    }
}
