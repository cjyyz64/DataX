package com.alibaba.datax.plugin.reader.hbasereader;

public final class Key {

    public final static String HBASE_CONFIG = "hbaseConfig";

    /**
     * mode 可以取 normal 或者 multiVersion 两个值，默认为 normal。二者区别非常大
     */
    public final static String MODE = "mode";

    /**
     * 配合 mode = mutiVersion 时使用，指明需要读取的版本个数。无默认值
     * -1 表示去读全部版本
     * 不能为0，1
     * >1表示最多读取对应个数的版本数(不能超过 Integer 的最大值)
     */
    public final static String MAX_VERSION = "maxVersion";

    public final static String ROWKEY_TYPE = "rowkeyType";

    public final static String ENCODING = "encoding";

    public final static String TABLE = "table";

    public final static String COLUMN = "column";

    public final static String START_ROWKEY = "startRowkey";

    public final static String END_ROWKEY = "endRowkey";

    public final static String IS_BINARY_ROWKEY = "isBinaryRowkey";

    public final static String SCAN_CACHE = "scanCache";

}
