package com.alibaba.datax.plugin.reader.sqlserverreader;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public class Key {

	// must
	public static final String JDBC_URL = "jdbcUrl";

	// must
	public static final String USERNAME = "username";

	// must
	public static final String PASSWORD = "password";

	// must
	public static final String TABLE = "table";

	// not must, it's a List, default all columns
	public static final String COLUMN = "column";

	// not must, default nothing
	public static final String SPLIT_PK = "splitPK";

	// not must, default nothing
	public static final String WHERE = "where";

	// not must, default nothing
	public static final String QUERYSQL = "querySql";
	
	public static final String FETCH_SIZE = "fetchSize";
}
