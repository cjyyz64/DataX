package com.alibaba.datax.plugin.writer.oceanbasewriter.utils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;


public class DateUtil {

    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static Timestamp parseTimestamp(String strDate, String format) {
        if (strDate == null) {
            return null;
        }
        try {
            Date date = getFormat(format).parse(strDate);
            return new Timestamp(date.getTime());
        } catch (Exception e) {
        	throw new IllegalArgumentException(String.format("parser Timestamp error [%s]-format[%s] ", strDate, format), e);
        }
    }

    private static final SimpleDateFormat getFormat(String format) {
        return new SimpleDateFormat(format);
    }
    
}