package com.alibaba.datax.plugin.rdbms.reader;

public final class Constant {
    public static final String PK_TYPE = "pkType";

    public static final Object PK_TYPE_STRING = "pkTypeString";

    public static final Object PK_TYPE_LONG = "pkTypeLong";

    public static String CONN_MARK = "connection";

    public static String TABLE_NUMBER_MARK = "tableNumber";

    public static String IS_TABLE_MODE = "isTableMode";

    public final static String FETCH_SIZE = "fetchSize";

    public static String QUERY_SQL_TEMPLATE_WITHOUT_WHERE = "select %s from %s ";

    public static String QUERY_SQL_TEMPLATE = "select %s from %s where (%s)";

    static final String MYSQL_TABLE_NAME_ERR1 = "Table";
    static final String MYSQL_TABLE_NAME_ERR2 = "doesn't exist";
    static final String MYSQL_INSERT_PRI = "SELECT command denied to user";
    static final String MYSQL_COLUMN1 = "Unknown column";
    static final String MYSQL_COLUMN2 = "field list";
    static final String MYSQL_WHERE = "where clause";

    static final String ORACLE_TABLE_NAME = "table or view does not exist";
    static final String ORACLE_INSERT_PRI = "insufficient privileges";
    static final String ORACLE_SQL = "invalid identifier";
}
