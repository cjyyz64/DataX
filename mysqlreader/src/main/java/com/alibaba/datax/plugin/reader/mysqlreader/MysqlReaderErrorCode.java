package com.alibaba.datax.plugin.reader.mysqlreader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by jingxing on 14-9-4.
 */
public enum MysqlReaderErrorCode implements ErrorCode {
	CONF_ERROR("MysqlReader-00", "username/password not filled."), 
	UNKNOWN_ERROR("MysqlReader-01", "todo"), 
	NOT_RECOMMENDED("MysqlReader-02","your config not recommended."), 
	TABLE_QUERYSQL_MIXED("MysqlReader-03", "your config has both table and querySql, error.")
	
	;

	private final String code;
	private final String description;

	private MysqlReaderErrorCode(String code, String description) {
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
