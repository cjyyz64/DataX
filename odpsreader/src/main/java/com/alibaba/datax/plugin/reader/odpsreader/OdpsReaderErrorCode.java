package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum OdpsReaderErrorCode implements ErrorCode {
	RUNTIME_EXCEPTION("OdpsReader-00", "run time exception"), NOT_SUPPORT_TYPE(
			"OdpsReader-01", "not supported column type");

	private final String code;
	private final String description;

	private OdpsReaderErrorCode(String code, String description) {
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
