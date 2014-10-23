package com.alibaba.datax.plugin.writer.mysqlwriter;

/**
 * 用于插件解析用户配置时，需要进行标识（MARK）的常量的声明.
 */
public final class Constant {
    public static final int DEFAULT_BATCH_SIZE = 32;

    public static String CONN_MARK = "connection";

    public static String TABLE_NUMBER_MARK = "tableNumber";

    public static String COLUMN_NUMBER_MARK = "columnNumber";

    public static String INSERT_OR_REPLACE_TEMPLATE_MARK = "insertOrReplaceTemplate";

}
