package com.alibaba.datax.plugin.reader.sqlserverreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum SqlServerReaderErrorCode implements ErrorCode {
	 REQUIRED_KEY("SqlServerReader-00", "lost necessary key")
	,REQUIRED_VALUE("SqlServerReader-01", "lost necessary value")
    ,ILLEGAL_KEY("SqlServerReader-02", "illegal key")
    ,ILLEGAL_VALUE("SqlServerReader-03", "illegal value")
    ,ILLEGAL_SPLIT_PK("SqlServerReader-04", "illegal split pk value")
    ,NOT_RECOMMENDED("SqlServerReader-05", "your config not recommended")
    ,SQL_EXECUTE_FAIL("SqlServerReader-06", "failed to execute sqlserver sql")
    ,READ_RECORD_FAIL("SqlServerReader-07", "failed to read sqlserver record")
    ,TABLE_QUERYSQL_MIXED("SqlServerReader-08", "can not config both table and querySql")
    ,TABLE_QUERYSQL_MISSING("SqlServerReader-09", "table and querySql should configured one item.")
    ,RUNTIME_EXCEPTION("SqlServerReader-10", "run time exception"),;

	private String code;
	private String description;

	private SqlServerReaderErrorCode(String code, String description) {
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

}
