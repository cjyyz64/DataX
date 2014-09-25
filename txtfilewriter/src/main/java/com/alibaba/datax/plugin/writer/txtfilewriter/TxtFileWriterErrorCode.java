package com.alibaba.datax.plugin.writer.txtfilewriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public enum TxtFileWriterErrorCode implements ErrorCode {
	RUNTIME_EXCEPTION("TxtFileWriter-00", "run time exception"), FILE_EXCEPTION(
			"TxtFileWriter-01", "file write exception"), CONFIG_INVALID_EXCEPTION(
			"TxtFileWriter-02", "config parameter is invalid"), ;

	private final String code;
	private final String description;

	private TxtFileWriterErrorCode(String code, String description) {
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
