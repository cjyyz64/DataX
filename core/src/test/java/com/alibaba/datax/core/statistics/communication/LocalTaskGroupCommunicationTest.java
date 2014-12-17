package com.alibaba.datax.core.statistics.communication;

import com.alibaba.datax.dataxservice.face.domain.State;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by jingxing on 14/12/1.
 */
public class LocalTaskGroupCommunicationTest {
    private final int taskGroupNumber = 5;

    @Before
    public void setUp() {
        LocalTaskGroupCommunicationManager.clear();
        for (int index = 0; index < taskGroupNumber; index++) {
            LocalTaskGroupCommunicationManager.registerTaskGroupCommunication(
                    index, new Communication());
        }
    }

    @Test
    public void LocalCommunicationTest() {
        Communication jobCommunication =
                LocalTaskGroupCommunicationManager.getJobCommunication();
        Assert.assertTrue(jobCommunication.getState().equals(State.RUNNING));

        for (int index : LocalTaskGroupCommunicationManager.getTaskGroupIdSet()) {
            Communication communication = LocalTaskGroupCommunicationManager
                    .getTaskGroupCommunication(index);
            communication.setState(State.SUCCEEDED);
            LocalTaskGroupCommunicationManager.updateTaskGroupCommunication(
                    index, communication);
        }

        jobCommunication = LocalTaskGroupCommunicationManager.getJobCommunication();
        Assert.assertTrue(jobCommunication.getState().equals(State.SUCCEEDED));
    }

    @Test(expected = IllegalArgumentException.class)
    public void noTaskGroupIdForUpdate() {
        LocalTaskGroupCommunicationManager.updateTaskGroupCommunication(
                this.taskGroupNumber + 1, new Communication());
    }

    @Test(expected = IllegalArgumentException.class)
    public void noTaskGroupIdForGet() {
        LocalTaskGroupCommunicationManager.getTaskGroupCommunication(-1);
    }
}
