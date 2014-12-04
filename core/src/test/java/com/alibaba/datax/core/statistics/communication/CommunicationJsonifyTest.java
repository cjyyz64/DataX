package com.alibaba.datax.core.statistics.communication;

import com.alibaba.datax.core.util.State;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by jingxing on 14/12/1.
 */
public class CommunicationJsonifyTest {
    @Test
    public void testJsonGetSnapshot() {
        Communication communication = new Communication();
        communication.setLongCounter(CommunicationManager.STAGE, 10);
        communication.setLongCounter(CommunicationManager.READ_SUCCEED_RECORDS, 100);
        communication.setLongCounter(CommunicationManager.READ_SUCCEED_BYTES, 102400);
        communication.setLongCounter(CommunicationManager.BYTE_SPEED, 10240);
        communication.setLongCounter(CommunicationManager.RECORD_SPEED, 100);
        communication.setDoubleCounter(CommunicationManager.PERCENTAGE, 0.1);
        communication.setState(State.RUN);
        communication.setLongCounter(CommunicationManager.WRITE_RECEIVED_RECORDS, 99);
        communication.setLongCounter(CommunicationManager.WRITE_RECEIVED_BYTES, 102300);

        String jsonString = CommunicationManager.Jsonify.getSnapshot(communication);
        JSONObject metricJson = JSON.parseObject(jsonString);

        Assert.assertEquals(communication.getLongCounter(CommunicationManager.RECORD_SPEED),
                metricJson.getLong("speedRecords"));
        Assert.assertTrue(
                Math.abs(communication.getDoubleCounter(CommunicationManager.PERCENTAGE)
                        - metricJson.getDouble("stage")) <= 0.001);
    }
}
