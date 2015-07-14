package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import org.junit.Test;


public class MultiVersionTimeRangeFunctiontest {
    
    /**
     * 测试目的：测试在默认参数的情况下，导出的数据是否符合预期
     * 测试内容：TimeRange不填，期望导出所有版本的数据，且值符合预期
     */
    @Test
    public void testDefaultTimeRange() {
        throw new RuntimeException("Unimplement");
    }
    
    /**
     * 测试目的：测试在设置参数的情况下，导出的数据是否符合预期
     * 测试内容：同时配置begin和end，期望导出的数据时间全部在begin和end之间，且值符合预期
     */
    @Test
    public void testCustomBeginAndEnd() {
        throw new RuntimeException("Unimplement");
    }
    
    /**
     * 测试目的：测试在设置参数的情况下，导出的数据是否符合预期
     * 测试内容：配置begin，期望导出的数据时间全部在begin和begin之后，且值符合预期
     */
    @Test
    public void testCustomBegin() {
        throw new RuntimeException("Unimplement");
    }
    
    /**
     * 测试目的：测试在设置参数的情况下，导出的数据是否符合预期
     * 测试内容：配置begin，期望导出的数据时间全部在end之前，且值符合预期
     */
    @Test
    public void testCustomEnd() {
        throw new RuntimeException("Unimplement");
    }
    
    /**
     * 测试目的：测试在设置参数的情况下，导出的数据是否符合预期
     * 测试内容：同时配置begin、end、maxVersion，期望导出的数据时间全部在begin和end之间，且同Column的数据在maxVersion限定之内，且值符合预期
     */
    @Test
    public void testCustomBeginAndEndAndMaxVersion() {
        throw new RuntimeException("Unimplement");
    }
}
