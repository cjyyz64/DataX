package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.dataxservice.face.domain.JobStatus;
import com.alibaba.datax.dataxservice.face.domain.Result;
import com.alibaba.datax.dataxservice.face.domain.TaskGroup;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupStatus;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;


public final class DataxServiceUtil {

    private static String basicUrl;
    private static int timeoutInMilliSeconds;
    private static Long jobId;

    public static void setBasicUrl(String basicUrl) {
        DataxServiceUtil.basicUrl = basicUrl;
    }

    public static void setTimeoutInMilliSeconds(int timeoutInMilliSeconds) {
        DataxServiceUtil.timeoutInMilliSeconds = timeoutInMilliSeconds;
    }

    public static void setJobId(Long jobId) {
        DataxServiceUtil.jobId = jobId;
    }

    private static HttpClientUtil httpClientUtil = HttpClientUtil.getHttpClientUtil();

    public static Result<Integer> getJobInfo(Long jobId) {
        String url = basicUrl + "inner/job/" + jobId + "/state";

        try {
            HttpGet httpGet = HttpClientUtil.getGetRequest();
            httpGet.setURI(new URI(url));
            String resJson = httpClientUtil.executeAndGetWithRetry(httpGet, 3, 1000l);

            Type type = new TypeToken<Result<Integer>>() {
            }.getType();
            Result<Integer> result = SerializationUtil.gson2Object(resJson, type);
            return result;

        } catch (Exception e) {
            System.err.println("getJobInfo error");
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "getJobInfo error");
        }
    }

    public static Result updateJobInfo(Long jobId, JobStatus jobStatus) {
        String url = basicUrl + "inner/job/" + jobId + "/status";
        try {
            HttpPut httpPut = HttpClientUtil.getPutRequest();
            httpPut.setURI(new URI(url));

            StringEntity jsonEntity = new StringEntity(SerializationUtil.gson2String(jobStatus));
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);

            String resJson = httpClientUtil.executeAndGetWithRetry(httpPut, 3, 1000l);
            Result result = SerializationUtil.gson2Object(resJson, Result.class);
            return result;
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "updateJobInfo error");
        }
    }

    public static Result<List<TaskGroup>> getTaskGroupInJob(Long jobId) {
        String url = basicUrl + "inner/job/" + jobId + "/taskGroup";

        try {
            HttpGet httpGet = HttpClientUtil.getGetRequest();
            httpGet.setURI(new URI(url));

            String resJson = httpClientUtil.executeAndGetWithRetry(httpGet, 3, 1000l);

            Type type = new TypeToken<Result<List<TaskGroup>>>() {
            }.getType();
            Result<List<TaskGroup>> result = SerializationUtil.longDateGson2Object(resJson, type);


            return result;
        } catch (Exception e) {
            System.err.println("getJobInfo error");
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "getJobInfo error");
        }
    }

    public static Result startTaskGroup(Long jobId, TaskGroup taskGroup) {
        String url = basicUrl + "inner/job/" + jobId + "/taskGroup";
        try {
            HttpPost httpPost = HttpClientUtil.getPostRequest();
            httpPost.setURI(new URI(url));

            StringEntity jsonEntity = new StringEntity(SerializationUtil.gson2String(taskGroup));
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPost.setEntity(jsonEntity);

            String resJson = httpClientUtil.executeAndGetWithRetry(httpPost, 3, 1000l);
            Result result = SerializationUtil.gson2Object(resJson, Result.class);
            return result;
        } catch (Exception e) {
            System.err.println("startTaskGroup error, groupId = " + taskGroup.getTaskGroupId());
            //throw new RuntimeException("startTaskGroup error");
            throw DataXException.asDataXException(FrameworkErrorCode.START_TASKGROUP_ERROR, "startTaskGroup error");
        }
    }

    public static Result killTaskGroup(Long jobId, Integer taskGroupId) {
        String url = basicUrl + "inner/job/" + jobId + "/taskGroup/" + taskGroupId;
        try {
            HttpDelete httpDelete = HttpClientUtil.getDeleteRequest();
            httpDelete.setURI(new URI(url));

            String resJson = httpClientUtil.executeAndGetWithRetry(httpDelete, 3, 1000l);
            Result result = SerializationUtil.gson2Object(resJson, Result.class);
            return result;

        } catch (Exception e) {
            System.err.println("killTaskGroup error");
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "killTaskGroup error");
        }
    }

    public static Result updateTaskGroupInfo(Long jobId, Integer taskGroupId, TaskGroupStatus taskGroupStatus) {
        String url = basicUrl + "inner/job/" + jobId + "/taskGroup/" + taskGroupId + "/status";
        try {
            HttpPut httpPut = HttpClientUtil.getPutRequest();
            httpPut.setURI(new URI(url));


            StringEntity jsonEntity = new StringEntity(SerializationUtil.gson2String(taskGroupStatus));
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);

            String resJson = httpClientUtil.executeAndGetWithRetry(httpPut, 3, 1000l);
            Result result = SerializationUtil.gson2Object(resJson, Result.class);
            return result;
        } catch (Exception e) {
            System.err.println("updateTaskGroupInfo error");
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "updateTaskGroupInfo error");
        }
    }

    public static JobStatus convertToJobStatus(String info) {
        if (StringUtils.isBlank(info)) {
            throw new IllegalArgumentException("can not convert null/empty to JobStatus.");
        }

        JobStatus jobStatus = SerializationUtil.gson2Object(info, JobStatus.class);

        return jobStatus;
    }

    /**
     * TODO 统计数据指标  update?
     */
    public static Communication convertTaskGroupToCommunication(TaskGroup taskGroup) {
        Communication communication = new Communication();
        communication.setState(taskGroup.getState());
        if (taskGroup.getTotalRecords() == null) {
            taskGroup.setTotalRecords(0L);
        }

        if (taskGroup.getTotalBytes() == null) {
            taskGroup.setTotalBytes(0L);
        }

        if (taskGroup.getErrorRecords() == null) {
            taskGroup.setErrorRecords(0L);
        }

        if (taskGroup.getErrorBytes() == null) {
            taskGroup.setErrorBytes(0L);
        }

        communication.setLongCounter("totalRecords", taskGroup.getTotalRecords());
        communication.setLongCounter(CommunicationManager.READ_SUCCEED_RECORDS, taskGroup.getTotalRecords());
        communication.setLongCounter("totalReadRecords", taskGroup.getTotalRecords());

        communication.setLongCounter("totalBytes", taskGroup.getTotalBytes());
        communication.setLongCounter(CommunicationManager.READ_SUCCEED_BYTES, taskGroup.getTotalBytes());
        communication.setLongCounter("totalReadBytes", taskGroup.getTotalBytes());

        communication.setLongCounter("errorRecords", taskGroup.getErrorRecords());
        communication.setLongCounter("errorBytes", taskGroup.getErrorBytes());

        String errorMessage = taskGroup.getErrorMessage();
        if (StringUtils.isBlank(errorMessage)) {
            communication.setThrowable(null);
        } else {
            communication.setThrowable(new Throwable(errorMessage));
        }
        return communication;
    }
}
