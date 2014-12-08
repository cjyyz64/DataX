package com.alibaba.datax.core.util;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * TODO: 根据现有日志数据分析各类错误，进行细化。
 * <p/>
 * <p>请不要格式化本类代码</p>
 */
public enum FrameworkErrorCode implements ErrorCode {

    INSTALL_ERROR("Framework-00", "DataX引擎安装错误, 请联系您的运维解决 ."),
    RUNTIME_ERROR("Framework-02", "DataX引擎运行过程出错，具体原因请参看DataX运行结束时的错误诊断信息  ."),
    CONFIG_ERROR("Framework-03", "DataX引擎配置错误，该问题通常是由于DataX安装错误引起，请联系您的运维解决 ."),
    ARGUMENT_ERROR("Framework-04", "DataX引擎运行错误，该问题通常是由于内部编程错误引起，请联系DataX开发团队解决 ."),

    PLUGIN_INSTALL_ERROR("Framework-10", "DataX插件安装错误, 该问题通常是由于DataX安装错误引起，请联系您的运维解决 ."),
    PLUGIN_NOT_FOUND("Framework-11", "DataX插件配置错误, 该问题通常是由于DataX安装错误引起，请联系您的运维解决 ."),
    PLUGIN_INIT_ERROR("Framework-12", "DataX插件初始化错误, 该问题通常是由于DataX安装错误引起，请联系您的运维解决 ."),
    PLUGIN_RUNTIME_ERROR("Framework-13", "DataX插件运行时出错, 具体原因请参看DataX运行结束时的错误诊断信息 ."),
    PLUGIN_DIRTY_DATA_LIMIT_EXCEED("Framework-14", "DataX传输脏数据超过用户预期，该错误通常是由于源端数据存在较多业务脏数据导致，请仔细检查DataX汇报的脏数据日志信息, 或者您可以适当调大脏数据阈值 ."),
    PLUGIN_SPLIT_ERROR("Framework-15", "DataX插件切分出错, 该问题通常是由于DataX各个插件编程错误引起，请联系DataX开发团队解决"),
    KILLED_EXIT_VALUE("Framework-143", "DataX 被 Kill 了"),;

    private final String code;

    private final String description;

    private FrameworkErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }

    /**
     * 通过 "Framework-143" 来标示 任务是 Killed 状态
     */
    public int toExitValue() {
        if ("Framework-143".equalsIgnoreCase(this.code)) {
            return 143;
        } else {
            return 1;
        }
    }

}
