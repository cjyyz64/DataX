package com.alibaba.datax.plugin.reader.txtfilereader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by haiwei.luo on 14-9-20.
 */
public enum TxtFileReaderErrorCode implements ErrorCode {
	RUNTIME_EXCEPTION("TxtFileReader-00", "运行时异常"), FILE_EXCEPTION(
			"TxtFileReader-01", "文件读取异常"), CONFIG_INVALID_EXCEPTION(
			"TxtFileReader-02", "参数配置错误"), NOT_SUPPORT_TYPE("TxtFileReader-03",
			"不支持的类型"), CAST_VALUE_TYPE_ERROR("TxtFileReader-04", "无法完成指定类型的转换"), SECURITY_EXCEPTION(
			"TxtFileReader-05", "缺少权限"), ;

	private final String code;
	private final String description;

	private TxtFileReaderErrorCode(String code, String description) {
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
