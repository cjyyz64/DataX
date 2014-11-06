package com.alibaba.datax.core.util;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * TODO: 根据现有日志数据分析各类错误，进行细化。
 * 
 * <p>请不要格式化本类代码</p>
 */
public enum FrameworkErrorCode implements ErrorCode {

	INSTALL_ERROR("Framework-00", "DataX框架安装错误."),
	ARGUMENT_ERROR("Framework-01", "DataX框架参数有误."),
	INNER_ERROR("Framework-02", "DataX框架操作出错."),
	CONFIG_ERROR("Framework-03", "DataX框架配置错误."),

	
	PLUGIN_INSTALL_ERROR("Framework-10", "DataX插件安装错误."),
	PLUGIN_NOT_FOUND("Framework-11", "DataX插件配置错误."),
	PLUGIN_INIT_ERROR("Framework-12", "DataX插件初始化错误."),
	PLUGIN_RUNTIME_ERROR("Framework-13", "DataX插件运行时出错."),
	PLUGIN_DIRTY_DATA_LIMIT_EXCEED("Framework-14", "DataX插件脏数据超过限制."),
	PLUGIN_SPLIT_ERROR("Framework-15", "DataX插件切分出错."),
	;

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

}
