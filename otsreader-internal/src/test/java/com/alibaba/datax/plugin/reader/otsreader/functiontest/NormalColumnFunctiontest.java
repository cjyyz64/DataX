package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import org.junit.Test;


public class NormalColumnFunctiontest {
    
    /**
     * 测试目的：测试在指定列的情况下，系统导出的Column是否正确
     * 测试内容：Column中指定10个存在的列，10个不存在的列，期望导出指定的列，且值符合预期
     */
    @Test
    public void testCustomColumn() {
        throw new RuntimeException("Unimplement");
    }
    
    /**
     * 测试目的：测试在指定列的情况下，系统导出的Column是否正确
     * 测试内容：Column中指定10个常量列，期望导出指定的列，且值符合预期
     */
    @Test
    public void testCustomConstColumn() {
        throw new RuntimeException("Unimplement");
    }
    
    /**
     * 测试目的：测试在指定列的情况下，系统导出的Column是否正确
     * 测试内容：Column中指定10个存在的列，10个不存在的列，10个常量列，期望导出指定的列，且值符合预期
     */
    @Test
    public void testCustomNormalAndConstColumn() {
        throw new RuntimeException("Unimplement");
    }
}
