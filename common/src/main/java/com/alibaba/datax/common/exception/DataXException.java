package com.alibaba.datax.common.exception;

import com.alibaba.datax.common.spi.ErrorCode;

public class DataXException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	private ErrorCode errorCode;

	public DataXException(ErrorCode errorCode, String errorMessage) {
		super(errorCode.toString() + " - " + errorMessage);
		this.errorCode = errorCode;
	}

	public DataXException(ErrorCode errorCode, Throwable cause) {
		super(errorCode.toString(), cause);
		this.errorCode = errorCode;
	}

	public DataXException(ErrorCode errorCode, String errorMessage,
			Throwable cause) {
		super(errorCode.toString() + " - " + errorMessage, cause);
		this.errorCode = errorCode;
	}

	public static DataXException asDataXException(ErrorCode errorCode,
			Throwable cause) {
		if (cause instanceof DataXException) {
			return (DataXException) cause;
		}
		return new DataXException(errorCode, cause.getMessage(), cause);
	}

	public ErrorCode getErrorCode() {
		return this.errorCode;
	}
}
