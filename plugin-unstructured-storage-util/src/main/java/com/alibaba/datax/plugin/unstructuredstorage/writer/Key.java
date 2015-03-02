package com.alibaba.datax.plugin.unstructuredstorage.writer;

public class Key {
    // must have
    public static final String FILE_NAME = "fileName";

    // must have
    public static final String WRITE_MODE = "writeMode";

    // not must , not default ,
    public static final String FIELD_DELIMITER = "fieldDelimiter";

    // not must, default UTF-8
    public static final String ENCODING = "encoding";

    // not must, default no compress
    public static final String COMPRESS = "compress";

    // not must, not default \N
    public static final String NULL_FORMAT = "nullFormat";

    // not must, date format
    public static final String FORMAT = "format";
    // for writers ' data format
    public static final String DATE_RORMAT = "dateFormat";
}
