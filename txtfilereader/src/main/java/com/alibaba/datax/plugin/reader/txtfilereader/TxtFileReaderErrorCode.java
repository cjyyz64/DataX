package com.alibaba.datax.plugin.reader.txtfilereader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by haiwei.luo on 14-9-20.
 */
public enum TxtFileReaderErrorCode implements ErrorCode {
	RUNTIME_EXCEPTION("TxtFileReader-00", "run time exception"), FILE_EXCEPTION(
			"TxtFileReader-01", "file read exception"), CONFIG_INVALID_EXCEPTION(
			"TxtFileReader-02", "config parameter is invalid"), NOT_SUPPORT_TYPE(
			"TxtFileReader-03", "not supported column type"), CAST_VALUE_TYPE_ERROR(
			"TxtFileReader-04", "can not cast value to pointed type"), ;

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
