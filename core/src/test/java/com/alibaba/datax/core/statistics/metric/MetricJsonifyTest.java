package com.alibaba.datax.core.statistics.metric;

import com.alibaba.datax.core.util.Status;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by jingxing on 14-9-25.
 */
public class MetricJsonifyTest {
    @Test
    public void testJsonGetSnapshot() {
        Metric metric = new Metric();
        metric.setStage(10);
        metric.setReadSucceedRecords(100);
        metric.setReadSucceedBytes(102400);
        metric.setByteSpeed(10240);
        metric.setRecordSpeed(100);
        metric.setPercentage(0.1);
        metric.setStatus(Status.RUN);
        metric.setWriteReceivedRecords(99);
        metric.setWriteReceivedBytes(102300);

        String jsonString = MetricJsonify.getSnapshot(metric);
        JSONObject metricJson = JSON.parseObject(jsonString);

        Assert.assertEquals(metric.getRecordSpeed(), (long)metricJson.getLong("speedRecords"));
        Assert.assertTrue(
                Math.abs(metric.getPercentage()-metricJson.getDouble("stage")) <= 0.001);
    }
}
