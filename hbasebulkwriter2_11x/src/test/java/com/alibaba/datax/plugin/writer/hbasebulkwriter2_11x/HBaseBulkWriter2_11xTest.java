package com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x;

import org.junit.Assert;
import org.junit.Test;

/**
 * no comments.
 * Created by liqiang on 16/4/4.
 */
public class HBaseBulkWriter2_11xTest {
    @Test
    public void testNormal() throws Exception {
        Class<?> cla2 = Class.forName("com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x.HBaseBulkWriter2_11x$Job");
        Assert.assertEquals(cla2.getName(),"com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x.HBaseBulkWriter2_11x$Job");
    }
}