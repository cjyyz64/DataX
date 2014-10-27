package com.alibaba.datax.core.container.runner;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractSlavePlugin;
import com.alibaba.datax.common.plugin.SlavePluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.metric.MetricManager;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.Status;

/**
 * Created by jingxing on 14-9-1.
 */
public abstract class AbstractRunner {
	private AbstractSlavePlugin plugin;

	private Configuration jobConf;

	private RunnerStatus runnerStatus = new RunnerStatus();

	private int slaveId;

	private int channelId;

	public AbstractRunner(AbstractSlavePlugin abstractSlavePlugin) {
		this.plugin = abstractSlavePlugin;
	}

	public void destroy() {
        if(this.plugin != null) {
            this.plugin.destroy();
            this.plugin = null;
        }
	}

	public AbstractSlavePlugin getPlugin() {
		return plugin;
	}

	public void setPlugin(AbstractSlavePlugin plugin) {
		this.plugin = plugin;
	}

	public Configuration getJobConf() {
		return jobConf;
	}

	public void setJobConf(Configuration jobConf) {
		this.jobConf = jobConf;
		this.plugin.setPluginJobConf(jobConf);
	}

	public void setSlavePluginCollector(SlavePluginCollector pluginCollector) {
		this.plugin.setSlavePluginCollector(pluginCollector);
	}

	private void mark(int status) {
		runnerStatus.setStatus(status);
	}

	public void markRun() {
		mark(Status.RUN.value());
	}

	public void markSuccess() {
		mark(Status.SUCCESS.value());
	}

	public void markFail(final Throwable throwable) {
		mark(Status.FAIL.value());

		MetricManager.getChannelMetric(this.getSlaveId(), this.getChannelId())
				.setError(throwable);

//		throw DataXException.asDataXException(
//				FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
	}

	public RunnerStatus getRunnerStatus() {
		return runnerStatus;
	}

	public void setRunnerStatus(RunnerStatus runnerStatus) {
		this.runnerStatus = runnerStatus;
	}

	/**
	 * @param slaveId
	 *            the slaveId to set
	 */
	public void setSlaveId(int slaveId) {
		this.slaveId = slaveId;
	}

	/**
	 * @param channelId
	 *            the channelId to set
	 */
	public void setChannelId(int channelId) {
		this.channelId = channelId;
	}

	/**
	 * @return the slaveId
	 */
	public int getSlaveId() {
		return slaveId;
	}

	/**
	 * @return the channelId
	 */
	public int getChannelId() {
		return channelId;
	}
}
