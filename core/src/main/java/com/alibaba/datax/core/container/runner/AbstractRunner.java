package com.alibaba.datax.core.container.runner;

import com.alibaba.datax.common.plugin.AbstractSlavePlugin;
import com.alibaba.datax.common.plugin.SlavePluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.util.State;
import org.apache.commons.lang.Validate;

/**
 * Created by jingxing on 14-9-1.
 */
public abstract class AbstractRunner {
    private AbstractSlavePlugin plugin;

    private Configuration jobConf;

    private Communication runnerCommunication;

    private int slaveContainerId;

    private int slaveExecutorId;

    public AbstractRunner(AbstractSlavePlugin abstractSlavePlugin) {
        this.plugin = abstractSlavePlugin;
    }

    public void destroy() {
        if (this.plugin != null) {
            this.plugin.destroy();
            this.plugin = null;
        }
    }

    public State getRunnerState() {
        return this.runnerCommunication.getState();
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

    private void mark(State state) {
        this.runnerCommunication.setState(state);
    }

    public void markRun() {
        mark(State.RUN);
    }

    public void markSuccess() {
        mark(State.SUCCESS);
    }

    public void markFail(final Throwable throwable) {
        mark(State.FAIL);

        this.runnerCommunication.setThrowable(throwable);
    }

    /**
     * @param slaveContainerId
     *            the slaveContainerId to set
     */
    public void setSlaveContainerId(int slaveContainerId) {
        this.slaveContainerId = slaveContainerId;
    }

    /**
     * @return the slaveContainerId
     */
    public int getSlaveContainerId() {
        return slaveContainerId;
    }

    public int getSlaveExecutorId() {
        return slaveExecutorId;
    }

    public void setSlaveExecutorId(int slaveExecutorId) {
        this.slaveExecutorId = slaveExecutorId;
    }

    public void setRunnerCommunication(final Communication runnerCommunication) {
        Validate.notNull(runnerCommunication, "插件的Communication不能为空");
        this.runnerCommunication = runnerCommunication;
    }
}
