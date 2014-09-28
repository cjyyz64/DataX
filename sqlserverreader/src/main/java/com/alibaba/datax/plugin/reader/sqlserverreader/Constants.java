package com.alibaba.datax.plugin.reader.sqlserverreader;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public class Constants {
	public static final int DEFAULT_FETCH_SIZE = 32;
	
	public static final String CONNECTION = "connection";
	
	public static String TABLE_NUMBER = "tableNumber";

	public static String TABLE_MODE = "tableMode";

	public static String QUERY_SQL_TEMPLATE_WHITOUT_WHERE = "select %s from %s ";

	public static String QUERY_SQL_TEMPLATE = "select %s from %s where (%s)";
}
