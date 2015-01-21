package com.alibaba.datax.common.plugin;

/**
 * Created by jingxing on 14-8-24.
 */
public abstract class AbstractTaskPlugin extends AbstractPlugin {
    private TaskPluginCollector taskPluginCollector;

    public TaskPluginCollector getTaskPluginCollector() {
        return taskPluginCollector;
    }

    public void setTaskPluginCollector(
            TaskPluginCollector taskPluginCollector) {
        this.taskPluginCollector = taskPluginCollector;
    }

}
