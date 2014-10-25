package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * 主要是测试CU不够和调整CU之后Writer的行为
 */
public class CUFunctiontest {

    /**
     * 输入：创建一个1CU的表，构造100行数据，每行数据都大于1CU，并将数据导入到OTS中
     * 期望：Writer重试一定时间之后异常退出
     */
    @Test
    public void test1() {
        assertTrue(false);
    }
    
    /**
     * 输入：创建一个2CU的表，构造100行数据，每行数据都大于1CU且小于2CU，并将数据导入到OTS中
     * 期望：数据能够被全部导入到OTS中，且数据正确
     */
    @Test
    public void test2() {
        assertTrue(false);
    }
}
