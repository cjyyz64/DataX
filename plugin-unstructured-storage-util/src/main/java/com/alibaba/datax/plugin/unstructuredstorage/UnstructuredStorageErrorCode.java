package com.alibaba.datax.plugin.unstructuredstorage;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by haiwei.luo on 14-9-20.
 */
public enum UnstructuredStorageErrorCode implements ErrorCode {
	CONFIG_INVALID_EXCEPTION("NoSQLReader-00", "您的参数配置错误."),
	NOT_SUPPORT_TYPE("NoSQLReader-01","您配置的列类型暂不支持."),
	REQUIRED_VALUE("NoSQLReader-02", "您缺失了必须填写的参数值."),
	ILLEGAL_VALUE("NoSQLReader-03", "您填写的参数值不合法."),
	MIXED_INDEX_VALUE("NoSQLReader-04", "您的列信息配置同时包含了index,value."),
	NO_INDEX_VALUE("NoSQLReader-05","您明确的配置列信息,但未填写相应的index,value."),
	FILE_NOT_EXISTS("NoSQLReader-06", "您配置的源路径不存在."),
	OPEN_FILE_WITH_CHARSET_ERROR("NoSQLReader-07", "您配置的编码和实际存储编码不符合."),
	OPEN_FILE_ERROR("NoSQLReader-08", "您配置的源在打开时异常,建议您检查源源是否有隐藏实体,管道文件等特殊文件."),
	READ_FILE_IO_ERROR("NoSQLReader-09", "您配置的文件在读取时出现IO异常."),
	SECURITY_NOT_ENOUGH("NoSQLReader-10", "您缺少权限执行相应的文件读取操作."),
	RUNTIME_EXCEPTION("NoSQLReader-11", "出现运行时异常, 请联系我们");

	private final String code;
	private final String description;

	private UnstructuredStorageErrorCode(String code, String description) {
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
		return String.format("Code:[%s], Description:[%s].", this.code,
				this.description);
	}
}
