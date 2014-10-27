package com.alibaba.datax.plugin.writer.otswriter.functiontest;

/**
 * 主要是测试PK不同类型之间的转换
 */
public class ConversionPKFunctiontest {
    // 传入值是String，用户指定的是String, 期待转换正常，且值符合预期
    // 传入值是Int，用户指定的是String, 期待转换正常，且值符合预期
    // 传入值是Double，用户指定的是String, 期待转换正常，且值符合预期
    // 传入值是Bool，用户指定的是String, 期待转换正常，且值符合预期
    // 传入值是Binary，用户指定的是String, 期待转换正常，且值符合预期
    
    // 传入值是String，用户指定的是Int, 期待转换正常，且值符合预期
    // 传入值是Int，用户指定的是Int, 期待转换正常，且值符合预期
    // 传入值是Double，用户指定的是Int, 期待转换正常，且值符合预期
    // 传入值是Bool，用户指定的是Int, 期待转换正常，且值符合预期
    
    // 传入值是Binary，用户指定的是Int, 期待转换异常，异常信息符合预期
    // 传入值是String，但是是非数值型的字符串，如“hello”， “100L”， “0x5f”，用户指定的是Int, 期待转换异常，异常信息符合预期
}
