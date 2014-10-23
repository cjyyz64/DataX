package com.alibaba.datax.common.exception;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 *
 */
public enum CommonErrorCode implements ErrorCode {

	CONVERT_NOT_SUPPORT("Common-00", "Element convert failed ."), CONFIG_ERROR(
			"Common-01", "Configuration error."), RETRY_FAIL("Common-02",
			"Retry to execute some method failed."), ;

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
