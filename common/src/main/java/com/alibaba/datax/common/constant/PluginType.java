package com.alibaba.datax.common.constant;

/**
 * Created by jingxing on 14-8-31.
 */
public enum PluginType {
    READER("reader"), TRANSFORMER("transformer"), WRITER("writer");

    private String pluginType;

    private PluginType(String pluginType) {
        this.pluginType = pluginType;
    }

    @Override
    public String toString() {
        return this.pluginType;
    }
}
