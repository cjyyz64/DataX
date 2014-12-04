package com.alibaba.datax.common.plugin;

/**
 * Created by jingxing on 14-8-24.
 */
public abstract class AbstractTaskPlugin extends AbstractPlugin {
	/**
	 * @return the taskPluginCollector
	 */
	public TaskPluginCollector getTaskPluginCollector() {
		return taskPluginCollector;
	}

	/**
	 * @param taskPluginCollector
	 *            the taskPluginCollector to set
	 */
	public void setTaskPluginCollector(
            TaskPluginCollector taskPluginCollector) {
		this.taskPluginCollector = taskPluginCollector;
	}

	private TaskPluginCollector taskPluginCollector;

}
