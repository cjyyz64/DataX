package com.alibaba.datax.common.util;

import java.text.DecimalFormat;

public class StrUtil {

    // 第一个%s 对应到：插件开发人员填充的业务信息。第二个%s 对应到异常的简短信息
    private static final String ORIGINAL_CAUSE__WITH_EXCEPTION_TEMPLATE = "Original cause: {%s},{%s}.";

    // %s 对应到：插件开发人员填充的业务信息
    private static final String ORIGINAL_CAUSE__WITHOUT_EXCEPTION_TEMPLATE = "Original cause: [%s].";

    private final static long KB_IN_BYTES = 1024;

    private final static long MB_IN_BYTES = 1024 * KB_IN_BYTES;

    private final static long GB_IN_BYTES = 1024 * MB_IN_BYTES;

    private final static long TB_IN_BYTES = 1024 * GB_IN_BYTES;

    private final static DecimalFormat df = new DecimalFormat("0.00");

    private static String SYSTEM_ENCODING = System.getProperty("file.encoding");

    static {
        if (SYSTEM_ENCODING == null) {
            SYSTEM_ENCODING = "UTF-8";
        }
    }

    private StrUtil() {
    }

    public static String stringify(long byteNumber) {
        if (byteNumber / TB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) TB_IN_BYTES) + "TB";
        } else if (byteNumber / GB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) GB_IN_BYTES) + "GB";
        } else if (byteNumber / MB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) MB_IN_BYTES) + "MB";
        } else if (byteNumber / KB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) KB_IN_BYTES) + "KB";
        } else {
            return String.valueOf(byteNumber) + "B";
        }
    }

    public static String buildOriginalCauseMessage(String businessMessage, Throwable thr) {
        if (null == thr) {
            return String.format(ORIGINAL_CAUSE__WITHOUT_EXCEPTION_TEMPLATE, businessMessage);
        } else {
            return String.format(ORIGINAL_CAUSE__WITH_EXCEPTION_TEMPLATE, businessMessage,
                    thr.getMessage());
        }
    }
}
