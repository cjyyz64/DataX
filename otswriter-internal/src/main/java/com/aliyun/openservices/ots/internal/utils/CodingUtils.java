package com.aliyun.openservices.ots.internal.utils;

import java.util.List;

/**
 * Utils for common coding.
 * 
 */
public class CodingUtils {

    public static void assertParameterNotNull(Object param, String paramName) {
        if (param == null) {
            throw new NullPointerException(String.format(
                    "ParameterIsNull:%s", paramName));
        }
    }

    public static void assertStringNotNullOrEmpty(String param, String paramName) {
        assertParameterNotNull(param, paramName);
        if (param.length() == 0) {
            throw new IllegalArgumentException(String.format(
                    "ParameterStringIsEmpty:%s", paramName));
        }
    }
    
    @SuppressWarnings("rawtypes")
    public static void assertListNotNullOrEmpty(List param, String paramName){
        assertParameterNotNull(param, paramName);
        if (param.size() == 0) {
            throw new IllegalArgumentException(String.format(
                    "ParameterListIsEmpty:%s", paramName));
        }
    }

    public static boolean isNullOrEmpty(String value) {
        return value == null || value.length() == 0;
    }
}
