package com.alibaba.datax.plugin.writer.swiftwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by zw86077 on 2015/12/9.
 */
public enum SwiftWriterErrorCode implements ErrorCode {


    CLIENT_CONFIG_ERROR("SwiftWriter-01", "没有配置 client_config"),
    WRITE_CONFIG_ERROR("SwiftWriter-02", "没有配置 writer_config"),
    INDEX_CONFIG_ERROR("SwiftWriter-04", "没有配置 index_names"),
    CLIENT_INIT_ERROR("SwiftWriter-05", "swiftClient 初始化失败,请检查 client_config 配置"),
    TOPIC_CREATE_ERROR("SwiftWriter-07", "Topic 创建失败"),
    WRITER_CREATE_ERROR("SwiftWriter-08", "writer 创建失败"),
    ILLEGAL_VALUE("SwiftWriter-09", "配置非法");

    SwiftWriterErrorCode(String code, String desc) {
        this.code = code;
        this.desc = desc;

    }


    private String code;

    private String desc;

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.desc;
    }
}
