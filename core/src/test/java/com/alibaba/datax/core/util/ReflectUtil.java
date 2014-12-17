package com.alibaba.datax.core.util;

import java.lang.reflect.Field;

/**
 * Created by hongjiao.hj on 2014/12/17.
 */
public class ReflectUtil {

    public static void setField(Object targetObj, String name, Object obj) throws NoSuchFieldException, IllegalAccessException {
        Class clazz = targetObj.getClass();
        Field field = clazz.getDeclaredField(name);
        field.setAccessible(true);
        field.set(targetObj, obj);
    }

    public static Object getField(Object targetObj, String name, Object obj) throws NoSuchFieldException, IllegalAccessException {
        Class clazz = targetObj.getClass();
        Field field = clazz.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(targetObj);
    }
}
