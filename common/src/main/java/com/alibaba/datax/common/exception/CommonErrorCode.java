package com.alibaba.datax.common.exception;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 *
 */
public enum CommonErrorCode implements ErrorCode {

	CONFIG_ERROR("Common-00", "配置错误，请检查您的作业或框架配置 ."), CONVERT_NOT_SUPPORT(
			"Common-01", "传输数据过程中发现类型转换失败 ."), CONVERT_OVER_FLOW("Common-02",
			"数据转换出现溢出，数据传输失真，请检查您的传输数据 ."), RETRY_FAIL("Common-10",
			"调用方法重试多次仍然失败."), ;

	private final String code;

	private final String describe;

	private CommonErrorCode(String code, String describe) {
		this.code = code;
		this.describe = describe;
	}

	@Override
	public String getCode() {
		return this.code;
	}

	@Override
	public String getDescription() {
		return this.describe;
	}

	@Override
	public String toString() {
		return String.format("Code:[%s], Describe:[%s]", this.code,
				this.describe);
	}

}
