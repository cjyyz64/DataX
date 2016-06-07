package com.alibaba.datax.plugin.reader.oceanbasev10reader;


public interface Config {
//queryTimeoutSecond
	String QUERY_TIMEOUT_SECOND = "memstoreCheckIntervalSecond";

	int DEFAULT_QUERY_TIMEOUT_SECOND = 60*60*48;// 2天
}
