package com.alibaba.datax.plugin.writer.ftpwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by haiwei.luo on 15-02-13.
 */
public enum FtpWriterErrorCode implements ErrorCode {
	
	CONFIG_INVALID_EXCEPTION("OssWriter-00", "您的参数配置错误."),
	REQUIRED_VALUE("OssWriter-01", "您缺失了必须填写的参数值."),
	ILLEGAL_VALUE("OssWriter-02", "您填写的参数值不合法."),
	Write_FILE_ERROR("OssWriter-03", "您配置的目标文件在写入时异常."),
	CONNECT_FTP_ERROR("OssWriter-04", "FTP连接异常."),
	LIST_FILES_ERROR("OssWriter-05", "查看文件异常."),
	;

	private final String code;
	private final String description;

	private FtpWriterErrorCode(String code, String description) {
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
