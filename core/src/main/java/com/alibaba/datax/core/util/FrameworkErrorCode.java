package com.alibaba.datax.core.util;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * TODO: 根据现有日志数据分析各类错误，进行细化。
 * 
 * <p>请不要格式化本类代码</p>
 */
public enum FrameworkErrorCode implements ErrorCode {

	INSTALL_ERROR("Framework-00", "DataX Framework Install Error ."), 
	ARGUMENT_ERROR("Framework-01","Core argument error ."), 
	INNER_ERROR("Framework-02","Core operation error ."), 
	CONFIG_ERROR("Framework-03", "Configuration error ."), 

	
	PLUGIN_INSTALL_ERROR("Framework-10","Plugin Install Error ."), 
	PLUGIN_NOT_FOUND("Framework-11","Plugin Config Not Found."),
	PLUGIN_INIT_ERROR("Framework-12",	"Plugin init error ."), 
	PLUGIN_RUNTIME_ERROR("Framework-13",	"Plugin runtime error ."), 
	PLUGIN_DIRTY_DATA_LIMIT_EXCEED("Framework-14","Plugin dirty records exceed limit ."), 
	PLUGIN_SPLIT_ERROR("Framework-15","Plugin split error ."),
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
