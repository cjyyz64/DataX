package com.alibaba.datax.plugin.reader.sqlserverreader;

/**
 * Created by haiwei.luo on 14-9-17.
 * 用于插件解析用户配置时，需要进行标识（MARK）的常量的声明.
 */
public class Constant {
	public static final int DEFAULT_FETCH_SIZE = 32;
	
	public static final String PK_TYPE = "pkType";

    public static final Object PK_TYPE_STRING = "pkTypeString";

    public static final Object PK_TYPE_LONG = "pkTypeLong";
	
	public static final String CONN_MARK = "connection";
	
	public static final String TABLE_NUMBER_MARK = "tableNumber";

	public static final String IS_TABLE_MODE = "isTableMode";
	
	public static final String QUERY_SQL_TEMPLATE_WHITOUT_WHERE = "select %s from %s ";

	public static final String QUERY_SQL_TEMPLATE = "select %s from %s where (%s)";
	
}
