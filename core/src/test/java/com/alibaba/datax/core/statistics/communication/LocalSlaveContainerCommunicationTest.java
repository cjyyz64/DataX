package com.alibaba.datax.core.statistics.communication;

import com.alibaba.datax.core.util.State;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by jingxing on 14/12/1.
 */
public class LocalSlaveContainerCommunicationTest {
    private final int slaveContainerNumber = 5;

    @Before
    public void setUp() {
        LocalSlaveContainerCommunication.clear();
        for(int index=0; index<slaveContainerNumber; index++) {
            LocalSlaveContainerCommunication.registerSlaveContainerCommunication(
                    index, new Communication());
        }
    }

    @Test
    public void LocalCommunicationTest() {
        Communication masterCommunication =
                LocalSlaveContainerCommunication.getMasterCommunication();
        Assert.assertTrue(masterCommunication.getState().equals(State.RUN));

        for(int index : LocalSlaveContainerCommunication.getSlaveContainerIdSet()) {
            Communication communication = LocalSlaveContainerCommunication
                    .getSlaveContainerCommunication(index);
            communication.setState(State.SUCCESS);
            LocalSlaveContainerCommunication.updateSlaveContainerCommunication(
                    index, communication);
        }

        masterCommunication = LocalSlaveContainerCommunication.getMasterCommunication();
        Assert.assertTrue(masterCommunication.getState().equals(State.SUCCESS));
    }

    @Test(expected = IllegalArgumentException.class)
    public void noSlaveContainerIdForUpdate() {
        LocalSlaveContainerCommunication.updateSlaveContainerCommunication(
                this.slaveContainerNumber+1, new Communication());
    }

    @Test(expected = IllegalArgumentException.class)
    public void noSlaveContainerIdForGet() {
        LocalSlaveContainerCommunication.getSlaveContainerCommunication(-1);
    }
}
