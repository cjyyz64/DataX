package com.alibaba.datax.common.plugin;

/**
 * Created by jingxing on 14-8-24.
 */
public abstract class AbstractMasterPlugin extends AbstractPlugin {
	/**
	 * @return the masterPluginCollector
	 */
	public MasterPluginCollector getMasterPluginCollector() {
		return masterPluginCollector;
	}

	/**
	 * @param masterPluginCollector
	 *            the masterPluginCollector to set
	 */
	public void setMasterPluginCollector(
			MasterPluginCollector masterPluginCollector) {
		this.masterPluginCollector = masterPluginCollector;
	}

	private MasterPluginCollector masterPluginCollector;

}
