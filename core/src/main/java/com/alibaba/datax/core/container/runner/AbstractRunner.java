package com.alibaba.datax.core.container.runner;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractSlavePlugin;
import com.alibaba.datax.common.plugin.SlavePluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.Status;

/**
 * Created by jingxing on 14-9-1.
 */
public abstract class AbstractRunner {
	private AbstractSlavePlugin plugin;

	private Configuration jobConf;

	private RunnerStatus runnerStatus = new RunnerStatus();

	public AbstractRunner(AbstractSlavePlugin abstractSlavePlugin) {
		this.plugin = abstractSlavePlugin;
	}

	public void destroy() {
		this.plugin.destroy();
		this.plugin = null;
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

	private void mark(int status, Exception exception) {
		runnerStatus.setStatus(status);
		runnerStatus.setException(exception);
	}

	public void markRun() {
		mark(Status.RUN.value(), null);
	}

	public void markSuccess() {
		mark(Status.SUCCESS.value(), null);
	}

	public void markFail(final Throwable throwable) {
		if (throwable instanceof Exception) {
			mark(Status.FAIL.value(), (Exception) throwable);
		} else {
			mark(Status.FAIL.value(), new Exception(throwable));
		}

		throw DataXException.asDataXException(
				FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
	}

	public RunnerStatus getRunnerStatus() {
		return runnerStatus;
	}

	public void setRunnerStatus(RunnerStatus runnerStatus) {
		this.runnerStatus = runnerStatus;
	}
}
