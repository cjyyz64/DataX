package com.alibaba.datax.plugin.writer.oceanbasev10writer;

public interface Config {

	String MEMSTORE_THRESHOLD = "memstoreThreshold";

	double DEFAULT_MEMSTORE_THRESHOLD = 0.75d;

	String MEMSTORE_CHECK_INTERVAL_SECOND = "memstoreCheckIntervalSecond";

	long DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND = 5;
	
	String FAIL_TRY_COUNT  = "failTryCount ";
	
	int DEFAULT_FAIL_TRY_COUNT = 100;
}
