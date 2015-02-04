package com.alibaba.datax.core.util;

import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.dataxservice.face.domain.JobStatusDto;
import com.alibaba.datax.dataxservice.face.domain.Result;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupDto;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupStatusDto;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;


/**
 * Created by hongjiao.hj on 2014/12/17.
 */
public class DataxServiceTest extends CaseInitializer {


    private String url = "http://localhost:8080/";

    @Test
    public void testStartTaskGroup() throws Exception {

        HttpClientUtil httpClientUtil = Mockito.mock(HttpClientUtil.class);
        when(httpClientUtil.executeAndGet(any(HttpRequestBase.class)))
                .thenReturn("{\n" +
                        "    \"isSuccess\": true,\n" +
                        "    \"errorCode\": 200,\n" +
                        "    \"message\": \"Create task group succeeded.\",\n" +
                        "    \"data\": null\n" +
                        "}");

        DataxServiceUtil.init(url, 3000);
        TaskGroupDto taskGroup = new TaskGroupDto();
        taskGroup.setJobId(148l);
        taskGroup.setTaskGroupId(1);
        taskGroup.setConfig("abc");

        ReflectUtil.setField(new DataxServiceUtil(),"httpClientUtil",httpClientUtil);
        Result result = DataxServiceUtil.startTaskGroup(148l,taskGroup);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void testKillTaskGroup() throws Exception {
        HttpClientUtil httpClientUtil = Mockito.mock(HttpClientUtil.class);
        when(httpClientUtil.executeAndGetWithRetry(any(HttpRequestBase.class),anyInt(),anyLong()))
                .thenReturn("{\n" +
                        "    \"isSuccess\": true,\n" +
                        "    \"errorCode\": 200,\n" +
                        "    \"message\": \"Kill task group succeeded.\",\n" +
                        "    \"data\": null\n" +
                        "}");

        DataxServiceUtil.init(url, 3000);
        ReflectUtil.setField(new DataxServiceUtil(),"httpClientUtil",httpClientUtil);
        Result result = DataxServiceUtil.killTaskGroup(148L,1);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void testGetJobState() throws Exception {
        HttpClientUtil httpClientUtil = Mockito.mock(HttpClientUtil.class);
        when(httpClientUtil.executeAndGetWithRetry(any(HttpRequestBase.class),anyInt(),anyLong()))
                .thenReturn("{\n" +
                        "    \"isSuccess\": true,\n" +
                        "    \"errorCode\": 200,\n" +
                        "    \"message\": \"\",\n" +
                        "    \"data\": 40\n" +
                        "}");

        DataxServiceUtil.init(url, 3000);
        ReflectUtil.setField(new DataxServiceUtil(),"httpClientUtil",httpClientUtil);
        Integer state = DataxServiceUtil.getJobInfo(148L).getData();
        Assert.assertTrue(state == 40);
    }

    @Test
    public void testUpdateJobStatus() throws Exception {
        HttpClientUtil httpClientUtil = Mockito.mock(HttpClientUtil.class);
        when(httpClientUtil.executeAndGetWithRetry(any(HttpRequestBase.class),anyInt(),anyLong()))
                .thenReturn("{\n" +
                        "    \"isSuccess\": true,\n" +
                        "    \"errorCode\": 200,\n" +
                        "    \"message\": \"testUpdateJobStatus succeeded.\",\n" +
                        "    \"data\": null\n" +
                        "}");

        DataxServiceUtil.init(url, 3000);
        ReflectUtil.setField(new DataxServiceUtil(), "httpClientUtil", httpClientUtil);
        JobStatusDto jobStatus = new JobStatusDto();
        jobStatus.setPercentage(0.80);
        DataxServiceUtil.updateJobInfo(148L,jobStatus);
    }

    @Test
    public void testGetTaskGroupInJob() throws Exception {
        HttpClientUtil httpClientUtil = Mockito.mock(HttpClientUtil.class);
        when(httpClientUtil.executeAndGetWithRetry(any(HttpRequestBase.class),anyInt(),anyLong()))
                .thenReturn("{\n" +
                        "    \"isSuccess\": true,\n" +
                        "    \"errorCode\": 200,\n" +
                        "    \"message\": \"\",\n" +
                        "    \"data\": [\n" +
                        "        {\n" +
                        "            \"id\": 2154,\n" +
                        "            \"jobId\": 148,\n" +
                        "            \"taskGroupId\": 1,\n" +
                        "            \"resourceGroup\": \"datax_distribute\",\n" +
                        "            \"config\": \"abc\",\n" +
                        "            \"submitTime\": 1418806556000,\n" +
                        "            \"startTime\": 1418806556000,\n" +
                        "            \"endTime\": 1418806556000,\n" +
                        "            \"logUrl\": \"/datax3/2014-12-17/\",\n" +
                        "            \"alisaExecuteId\": \"T3_0001463400\",\n" +
                        "            \"alisaExecuteIp\": \"v101088139.sqa.zmf\",\n" +
                        "            \"state\": \"FAILED\",\n" +
                        "            \"stage\": null,\n" +
                        "            \"totalRecords\": null,\n" +
                        "            \"totalBytes\": null,\n" +
                        "            \"speedRecords\": null,\n" +
                        "            \"speedBytes\": null,\n" +
                        "            \"errorRecords\": null,\n" +
                        "            \"errorBytes\": null,\n" +
                        "            \"errorCode\": null,\n" +
                        "            \"errorMessage\": null,\n" +
                        "            \"message\": null\n" +
                        "        }\n" +
                        "    ]\n" +
                        "}");

        DataxServiceUtil.init(url, 3000);
        ReflectUtil.setField(new DataxServiceUtil(), "httpClientUtil", httpClientUtil);

        Result<List<TaskGroupDto>> result = DataxServiceUtil.getTaskGroupInJob(152L);
        Assert.assertTrue(result.getData().get(0).getResourceGroup().equals("datax_distribute"));
        /*String url = "http://localhost:8080/";
        DataxServiceUtil.init(url,3000);
        Result<List<TaskGroup>> result = DataxServiceUtil.getTaskGroupInJob(148L);
        System.out.println(result);*/
    }

    @Test
    public void testUpdateTaskGroupStatus() throws Exception {
        HttpClientUtil httpClientUtil = Mockito.mock(HttpClientUtil.class);
        when(httpClientUtil.executeAndGetWithRetry(any(HttpRequestBase.class),anyInt(),anyLong()))
                .thenReturn("{\n" +
                        "    \"isSuccess\": true,\n" +
                        "    \"errorCode\": 200,\n" +
                        "    \"message\": \"testUpdateJobStatus succeeded.\",\n" +
                        "    \"data\": null\n" +
                        "}");
        DataxServiceUtil.init(url, 3000);
        ReflectUtil.setField(new DataxServiceUtil(), "httpClientUtil", httpClientUtil);
        TaskGroupStatusDto taskGroupStatus = new TaskGroupStatusDto();
        taskGroupStatus.setJobId(148L);
        taskGroupStatus.setTaskGroupId(1);
        taskGroupStatus.setSpeedRecords(9999L);
        DataxServiceUtil.updateTaskGroupInfo(148L, 1, taskGroupStatus);
    }

    @Test
    public void testConvertTaskGroupToCommunication() {
        TaskGroupStatusDto taskGroupStatus = new TaskGroupStatusDto();
        taskGroupStatus.setJobId(1L);
        taskGroupStatus.setTaskGroupId(1);
        taskGroupStatus.setSpeedBytes(null);
        taskGroupStatus.setTotalRecords(null);
        taskGroupStatus.setTotalBytes(null);
        taskGroupStatus.setErrorRecords(null);
        taskGroupStatus.setErrorBytes(null);

        Communication communication = DataxServiceUtil.convertTaskGroupToCommunication(taskGroupStatus);
        Assert.assertTrue(communication.getLongCounter("totalRecords").equals(taskGroupStatus.getTotalRecords()));
        Assert.assertTrue(communication.getLongCounter("totalBytes").equals(taskGroupStatus.getTotalBytes()));
        Assert.assertTrue(communication.getLongCounter("errorRecords").equals(taskGroupStatus.getErrorRecords()));
        Assert.assertTrue(communication.getLongCounter("errorBytes").equals(taskGroupStatus.getErrorBytes()));
    }
}
