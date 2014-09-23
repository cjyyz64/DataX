package com.alibaba.datax.plugin.writer.mysqlwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by jingxing on 14-9-4.
 */
public enum MysqlWriterErrorCode implements ErrorCode {
	CONF_ERROR("MysqlWriter-00", "username/password not filled."), UNKNOWN_ERROR(
			"MysqlWriter-01", "todo"), NOT_RECOMMENDED("MysqlReader-02",
			"your config not recommended."), TABLE_QUERYSQL_MIXED(
			"MysqlWriter-03", "your config has both table and querySql, error.");

	private final String code;
	private final String describe;

	private MysqlWriterErrorCode(String code, String describe) {
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
		return String.format("Code:[%s], Describe:[%s]. ", this.code,
				this.describe);
	}
}
