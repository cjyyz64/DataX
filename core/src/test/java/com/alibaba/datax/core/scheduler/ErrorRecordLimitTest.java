package com.alibaba.datax.core.scheduler;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.metric.Metric;
import org.junit.Test;

public class ErrorRecordLimitTest {

    @Test(expected = DataXException.class)
    public void testCheckRecordLimit() throws Exception {
        ErrorRecordLimit errLimit = new ErrorRecordLimit(0L, 0.5);
        errLimit.checkRecordLimit(new Metric() {
            {
                this.setWriteFailedRecords(1);
            }
        });
    }

    @Test
    public void testCheckRecordLimit2() throws Exception {
        ErrorRecordLimit errLimit = new ErrorRecordLimit(1L, 0.5);
        errLimit.checkRecordLimit(new Metric() {
            {
                this.setWriteFailedRecords(1);
            }
        });
    }

    @Test
    public void testCheckRecordLimit3() throws Exception {
        // 百分数无效
        ErrorRecordLimit errLimit = new ErrorRecordLimit(1L, 0.05);
        errLimit.checkPercentageLimit(new Metric() {
            {
                this.setReadSucceedRecords(100);
                this.setWriteFailedRecords(50);
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidConstruction() throws Exception {
        new ErrorRecordLimit(-1L, 0.1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidConstruction2() throws Exception {
        new ErrorRecordLimit(0L, 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidConstruction3() throws Exception {
        new ErrorRecordLimit(0L, 1.1);
    }
}