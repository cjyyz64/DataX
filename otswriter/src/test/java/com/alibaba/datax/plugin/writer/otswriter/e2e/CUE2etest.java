package com.alibaba.datax.plugin.writer.otswriter.e2e;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * 主要是测试CU不够和调整CU之后Writer的行为
 */
public class CUE2etest {
    
    /**
     * 输入：创建一个1CU的表，构造100行数据，每行数据都大于1CU，并将数据导入到OTS中，在导入数据的过程中，将表的CU调大
     * 期望：数据能够被全部导入到OTS中，且数据正确
     */
    @Test
    public void test3() {
        assertTrue(false);
    }
    
    /**
     * 输入：创建一个2CU的表，构造100行数据，每行数据都大于1CU且小于2CU，并将数据导入到OTS中，在导入数据的过程中，
     *      将表的CU调小为1CU，运行一段时间之后，恢复表的CU
     * 期望：数据能够被全部导入到OTS中，且数据正确
     */
    @Test
    public void test4() {
        assertTrue(false);
    }
}
