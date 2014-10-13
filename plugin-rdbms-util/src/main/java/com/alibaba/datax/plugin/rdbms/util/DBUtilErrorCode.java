package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.spi.ErrorCode;

public enum DBUtilErrorCode implements ErrorCode {
	CONN_DB_ERROR("DBUtilErrorCode-00", "Connect to DataBase failed."), 
	COLUMN_SPLIT_ERROR(
			"DBUtilErrorCode-10", "Column split failed ."),

	;

	private final String code;

	private final String description;

	private DBUtilErrorCode(String code, String description) {
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
