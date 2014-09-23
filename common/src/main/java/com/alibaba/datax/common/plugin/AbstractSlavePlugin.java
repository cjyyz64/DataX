package com.alibaba.datax.common.plugin;

/**
 * Created by jingxing on 14-8-24.
 */
public abstract class AbstractSlavePlugin extends AbstractPlugin {
	/**
	 * @return the slavePluginCollector
	 */
	public SlavePluginCollector getSlavePluginCollector() {
		return slavePluginCollector;
	}

	/**
	 * @param slavePluginCollector
	 *            the slavePluginCollector to set
	 */
	public void setSlavePluginCollector(
			SlavePluginCollector slavePluginCollector) {
		this.slavePluginCollector = slavePluginCollector;
	}

	private SlavePluginCollector slavePluginCollector;

}
