package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.dataxservice.face.domain.JobStatus;
import com.alibaba.datax.dataxservice.face.domain.Result;
import com.alibaba.datax.dataxservice.face.domain.TaskGroup;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupStatus;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
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
    private static String DATAX_SERVICE_URL;

    private static HttpClientUtil httpClientUtil;

    public static void init(String dataxServiceUrl, int httpTimeOutInMillionSeconds) {
        DATAX_SERVICE_URL = dataxServiceUrl;
        HttpClientUtil.setHttpTimeoutInMillionSeconds(httpTimeOutInMillionSeconds);

        httpClientUtil = HttpClientUtil.getHttpClientUtil();
    }


    public static Result<Integer> getJobInfo(Long jobId) {
        String url = DATAX_SERVICE_URL + "inner/job/" + jobId + "/state";

        try {
            HttpGet httpGet = HttpClientUtil.getGetRequest();
            httpGet.setURI(new URI(url));
            String resJson = httpClientUtil.executeAndGetWithRetry(httpGet, 9, 1000l);

            Type type = new TypeReference<Result<Integer>>() {}.getType();
            Result<Integer> result = JSON.parseObject(resJson,type);

            if (!result.isSuccess()) {
                throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                        String.format("getJobInfo error, jobId=[%s], http result:[%s].", jobId, resJson));
            }
            return result;

        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED, String.format("getJobInfo error, jobId=[%s]", jobId), e);
        }
    }

    public static Result updateJobInfo(Long jobId, JobStatus jobStatus) {
        String url = DATAX_SERVICE_URL + "inner/job/" + jobId + "/status";
        try {
            HttpPut httpPut = HttpClientUtil.getPutRequest();
            httpPut.setURI(new URI(url));

            StringEntity jsonEntity = new StringEntity(JSON.toJSONString(jobStatus), "UTF-8");
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);

            // 这里重试次数为9，则能避免 DataXService 在7分钟不可用时，任务不会因此而失败.
            String resJson = httpClientUtil.executeAndGetWithRetry(httpPut, 9, 1000l);

            Type type = new TypeReference<Result<Object>>() {}.getType();
            Result result = JSON.parseObject(resJson,type);

            if (!result.isSuccess()) {
                throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                        String.format("updateJobInfo error, jobId=[%s], jobStatus=[%s], http result:[%s].", jobId, jobStatus, resJson));
            }

            return result;
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                    String.format("updateJobInfo error, jobId=[%s], jobStatus=[%s].", jobId, jobStatus.toString()), e);
        }
    }

    public static Result<List<TaskGroup>> getTaskGroupInJob(Long jobId) {
        String url = DATAX_SERVICE_URL + "inner/job/" + jobId + "/taskGroup";

        try {
            HttpGet httpGet = HttpClientUtil.getGetRequest();
            httpGet.setURI(new URI(url));
            String resJson = httpClientUtil.executeAndGetWithRetry(httpGet, 9, 1000l);

            Result<List<TaskGroup>> result = JSON.parseObject(resJson,
                    new TypeReference<Result<List<TaskGroup>>>(){});

            if (!result.isSuccess()) {
                throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                        String.format("getTaskGroupInJob error, jobId=[%s], http result:[%s].", jobId, resJson));
            }

            return result;
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                    String.format("getTaskGroupInJob error, jobId=[%s]", jobId), e);
        }
    }

    public static Result<List<TaskGroupStatus>> getTaskGroupStatusInJob(Long jobId) {
        String url = DATAX_SERVICE_URL + "inner/job/" + jobId + "/taskGroup/status";

        try {
            HttpGet httpGet = HttpClientUtil.getGetRequest();
            httpGet.setURI(new URI(url));
            String resJson = httpClientUtil.executeAndGetWithRetry(httpGet, 9, 1000l);

            Result<List<TaskGroupStatus>> result = JSON.parseObject(resJson,
                    new TypeReference<Result<List<TaskGroupStatus>>>(){});

            if (!result.isSuccess()) {
                throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                        String.format("getTaskGroupStatusInJob error, jobId=[%s], http result:[%s].", jobId, resJson));
            }

            return result;
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                    String.format("getTaskGroupStatusInJob error, jobId=[%s]", jobId), e);
        }
    }

    public static Result startTaskGroup(Long jobId, TaskGroup taskGroup) {
        String url = DATAX_SERVICE_URL + "inner/job/" + jobId + "/taskGroup";
        try {
            HttpPost httpPost = HttpClientUtil.getPostRequest();
            httpPost.setURI(new URI(url));

            StringEntity jsonEntity = new StringEntity(JSON.toJSONString(taskGroup), "UTF-8");
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPost.setEntity(jsonEntity);

            //String resJson = httpClientUtil.executeAndGetWithRetry(httpPost, 3, 1000l);
            //提交taskGroup不重试,防止重跑
            String resJson = httpClientUtil.executeAndGet(httpPost);
            Type type = new TypeReference<Result<Object>>() {}.getType();
            Result result = JSON.parseObject(resJson,type);
            if (!result.isSuccess()) {
                throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                        String.format("startTaskGroup error, jobId=[%s], taskGroup=[%s], http result:[%s].", jobId, taskGroup.toSimpleString(), resJson));
            }

            return result;
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                    String.format("startTaskGroup error, jobId=[%s], taskGroup=[%s].", jobId, taskGroup.toSimpleString()), e);
        }
    }

    public static Result killTaskGroup(Long jobId, Integer taskGroupId) {
        String url = DATAX_SERVICE_URL + "inner/job/" + jobId + "/taskGroup/" + taskGroupId;
        try {
            HttpDelete httpDelete = HttpClientUtil.getDeleteRequest();
            httpDelete.setURI(new URI(url));

            String resJson = httpClientUtil.executeAndGetWithRetry(httpDelete, 9, 1000l);

            Type type = new TypeReference<Result<Object>>() {}.getType();
            Result result = JSON.parseObject(resJson,type);

            if (!result.isSuccess()) {
                throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                        String.format("killTaskGroup error, jobId=[%s], taskGroupId=[%s], http result:[%s].", jobId, taskGroupId, resJson));
            }

            return result;

        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                    String.format("killTaskGroup error, jobId=[%s], taskGroupId=[%s].", jobId, taskGroupId), e);
        }
    }

    public static Result updateTaskGroupInfo(Long jobId, Integer taskGroupId, TaskGroupStatus taskGroupStatus) {
        String url = DATAX_SERVICE_URL + "inner/job/" + jobId + "/taskGroup/" + taskGroupId + "/status";
        try {
            HttpPut httpPut = HttpClientUtil.getPutRequest();
            httpPut.setURI(new URI(url));


            StringEntity jsonEntity = new StringEntity(JSON.toJSONString(taskGroupStatus), "UTF-8");
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);

            String resJson = httpClientUtil.executeAndGetWithRetry(httpPut, 9, 1000l);

            Type type = new TypeReference<Result<Object>>() {}.getType();
            Result result = JSON.parseObject(resJson,type);

            if (!result.isSuccess()) {
                throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                        String.format("updateTaskGroupInfo error, jobId=[%s], taskGroupId=[%s], TaskGroupStatus=[%s], http result:[%s].", jobId, taskGroupId, taskGroupStatus, resJson));
            }

            return result;
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                    String.format("updateTaskGroupInfo error, jobId=[%s], taskGroupId=[%s], TaskGroupStatus=[%s].", jobId, taskGroupId, taskGroupStatus), e);
        }
    }


    public static Communication convertTaskGroupToCommunication(TaskGroupStatus taskGroupStatus) {
        Communication communication = new Communication();
        communication.setState(taskGroupStatus.getState());
        if (taskGroupStatus.getStage() == null) {
            taskGroupStatus.setStage(0);
        }

        if (taskGroupStatus.getTotalRecords() == null) {
            taskGroupStatus.setTotalRecords(0L);
        }

        if (taskGroupStatus.getTotalBytes() == null) {
            taskGroupStatus.setTotalBytes(0L);
        }

        if (taskGroupStatus.getErrorRecords() == null) {
            taskGroupStatus.setErrorRecords(0L);
        }

        if (taskGroupStatus.getErrorBytes() == null) {
            taskGroupStatus.setErrorBytes(0L);
        }

        communication.setLongCounter("stage", taskGroupStatus.getStage());

        communication.setLongCounter("totalRecords", taskGroupStatus.getTotalRecords());
        communication.setLongCounter(CommunicationTool.READ_SUCCEED_RECORDS,
                taskGroupStatus.getTotalRecords() - taskGroupStatus.getErrorRecords());
        communication.setLongCounter("totalReadRecords", taskGroupStatus.getTotalRecords());

        communication.setLongCounter("totalBytes", taskGroupStatus.getTotalBytes());
        communication.setLongCounter(CommunicationTool.READ_SUCCEED_BYTES,
                taskGroupStatus.getTotalBytes() - taskGroupStatus.getErrorBytes());
        communication.setLongCounter("totalReadBytes", taskGroupStatus.getTotalBytes());

        communication.setLongCounter("readFailedRecords", taskGroupStatus.getErrorRecords());
        communication.setLongCounter("writeFailedRecords", 0);
        communication.setLongCounter("totalErrorRecords", taskGroupStatus.getErrorRecords());
        communication.setLongCounter("errorRecords", taskGroupStatus.getErrorRecords());

        communication.setLongCounter("readFailedBytes", taskGroupStatus.getErrorBytes());
        communication.setLongCounter("writeFailedBytes", 0);
        communication.setLongCounter("errorBytes", taskGroupStatus.getErrorBytes());
        communication.setLongCounter("totalErrorBytes", taskGroupStatus.getErrorBytes());

        String errorMessage = taskGroupStatus.getErrorMessage();
        if (StringUtils.isNotBlank(errorMessage)) {
            communication.setThrowable(new Throwable(errorMessage));
        }

        return communication;
    }

}
