package com.alibaba.datax.core.manager;


import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.dataxservice.face.domain.JobStatus;
import com.alibaba.datax.dataxservice.face.domain.Result;
import com.alibaba.datax.dataxservice.face.domain.TaskGroup;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupStatus;
import org.junit.Test;

import javax.xml.crypto.Data;
import java.util.List;


/**
 * Created by hongjiao.hj on 2014/12/9.
 */
public class DataServiceManagerTest {

    @Test
    public void testStartTaskGroup() {
        String url = "http://localhost:8080/";
        DataxServiceUtil.setBasicUrl(url);
        DataxServiceUtil.setTimeoutInMilliSeconds(1000);
        TaskGroup taskGroup = new TaskGroup();
        taskGroup.setJobId(148l);
        taskGroup.setTaskGroupId(1);
        taskGroup.setConfig("abc");
        DataxServiceUtil.startTaskGroup(148l,taskGroup);
    }

    @Test
    public void testKillTaskGroup() {
        String url = "http://localhost:8080/";
        DataxServiceUtil.setBasicUrl(url);
        DataxServiceUtil.setTimeoutInMilliSeconds(1000);
        DataxServiceUtil.killTaskGroup(148L,1);
    }

    @Test
    public void testGetJobState() {
        String url = "http://localhost:8080/";
        DataxServiceUtil.setBasicUrl(url);
        DataxServiceUtil.setTimeoutInMilliSeconds(1000);
        System.out.println(DataxServiceUtil.getJobInfo(148L).getData());
    }

    @Test
    public void testUpdateJobStatus() {
        String url = "http://localhost:8080/";
        DataxServiceUtil.setBasicUrl(url);
        DataxServiceUtil.setTimeoutInMilliSeconds(1000);
        JobStatus jobStatus = new JobStatus();
        jobStatus.setPercentage(0.80);
        DataxServiceUtil.updateJobInfo(148L,jobStatus);

    }

    @Test
    public void testGetTaskGroupInJob() {
        String url = "http://localhost:8080/";
        DataxServiceUtil.setBasicUrl(url);
        DataxServiceUtil.setTimeoutInMilliSeconds(1000);

        Result<List<TaskGroup>> result = DataxServiceUtil.getTaskGroupInJob(152L);
        System.out.println(result.getData().get(0).toString());
    }

    @Test
    public void testUpdateTaskGroupStatus() {
        String url = "http://localhost:8080/";
        DataxServiceUtil.setBasicUrl(url);
        DataxServiceUtil.setTimeoutInMilliSeconds(1000);
        TaskGroupStatus taskGroupStatus = new TaskGroupStatus();
        taskGroupStatus.setJobId(148L);
        taskGroupStatus.setTaskGroupId(1);
        taskGroupStatus.setSpeedRecords(9999L);
        DataxServiceUtil.updateTaskGroupInfo(148L, 1, taskGroupStatus);
    }
}
