package com.alibaba.datax.core.statistics.collector.plugin.slave;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.SlavePluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.FrameworkErrorCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jingxing on 14-9-11.
 */
public abstract class AbstractSlavePluginCollector extends SlavePluginCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractSlavePluginCollector.class);

    private Communication communication;

    private Configuration configuration;

    private PluginType pluginType;

    public AbstractSlavePluginCollector(Configuration conf, Communication communication,
                                        PluginType type) {
        this.configuration = conf;
        this.communication = communication;
        this.pluginType = type;
    }

    public Communication getCommunication() {
        return communication;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public PluginType getPluginType() {
        return pluginType;
    }

    @Override
    final public void collectMessage(String key, String value) {
        this.communication.addMessage(key, value);
    }

    @Override
    public void collectDirtyRecord(Record dirtyRecord, Throwable t,
                                   String errorMessage) {

        if (null == dirtyRecord) {
            LOG.warn("脏数据record=null.");
            return;
        }

        if (this.pluginType.equals(PluginType.READER)) {
            this.communication.increaseCounter(
                    CommunicationManager.READ_FAILED_RECORDS, 1);
            this.communication.increaseCounter(
                    CommunicationManager.READ_FAILED_BYTES, dirtyRecord.getByteSize());
        } else if (this.pluginType.equals(PluginType.WRITER)) {
            this.communication.increaseCounter(
                    CommunicationManager.WRITE_FAILED_RECORDS, 1);
            this.communication.increaseCounter(
                    CommunicationManager.WRITE_FAILED_BYTES, dirtyRecord.getByteSize());
        } else {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR,
                    String.format("不知道的插件类型[%s].", this.pluginType));
        }
    }
}
