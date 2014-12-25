package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
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
            String resJson = httpClientUtil.executeAndGetWithRetry(httpGet, 3, 1000l);

            Type type = new TypeToken<Result<Integer>>() {
            }.getType();
            Result<Integer> result = SerializationUtil.gson2Object(resJson, type);
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

            StringEntity jsonEntity = new StringEntity(SerializationUtil.gson2String(jobStatus));
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);

            // 这里重试次数为9，则能避免 DataXService 在7分钟不可用时，任务不会因此而失败.
            String resJson = httpClientUtil.executeAndGetWithRetry(httpPut, 9, 1000l);
            Result result = SerializationUtil.gson2Object(resJson, Result.class);

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

            Type type = new TypeToken<Result<List<TaskGroup>>>() {
            }.getType();
            Result<List<TaskGroup>> result = SerializationUtil.longDateGson2Object(resJson, type);

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

    public static Result startTaskGroup(Long jobId, TaskGroup taskGroup) {
        String url = DATAX_SERVICE_URL + "inner/job/" + jobId + "/taskGroup";
        try {
            HttpPost httpPost = HttpClientUtil.getPostRequest();
            httpPost.setURI(new URI(url));

            StringEntity jsonEntity = new StringEntity(SerializationUtil.gson2String(taskGroup));
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPost.setEntity(jsonEntity);

            String resJson = httpClientUtil.executeAndGetWithRetry(httpPost, 3, 1000l);
            Result result = SerializationUtil.gson2Object(resJson, Result.class);
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
            Result result = SerializationUtil.gson2Object(resJson, Result.class);
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


            StringEntity jsonEntity = new StringEntity(SerializationUtil.gson2String(taskGroupStatus));
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);

            String resJson = httpClientUtil.executeAndGetWithRetry(httpPut, 9, 1000l);
            Result result = SerializationUtil.gson2Object(resJson, Result.class);
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


    public static Communication convertTaskGroupToCommunication(TaskGroup taskGroup) {
        Communication communication = new Communication();
        communication.setState(taskGroup.getState());
        if(taskGroup.getStage() == null) {
            taskGroup.setStage(0);
        }

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

        communication.setLongCounter("stage",taskGroup.getStage());

        communication.setLongCounter("totalRecords", taskGroup.getTotalRecords());
        communication.setLongCounter(CommunicationTool.READ_SUCCEED_RECORDS,
                taskGroup.getTotalRecords()-taskGroup.getErrorRecords());
        communication.setLongCounter("totalReadRecords", taskGroup.getTotalRecords());

        communication.setLongCounter("totalBytes", taskGroup.getTotalBytes());
        communication.setLongCounter(CommunicationTool.READ_SUCCEED_BYTES,
                taskGroup.getTotalBytes()-taskGroup.getErrorBytes());
        communication.setLongCounter("totalReadBytes", taskGroup.getTotalBytes());

        communication.setLongCounter("readFailedRecords",taskGroup.getErrorRecords());
        communication.setLongCounter("writeFailedRecords",0);
        communication.setLongCounter("totalErrorRecords",taskGroup.getErrorRecords());
        communication.setLongCounter("errorRecords", taskGroup.getErrorRecords());

        communication.setLongCounter("readFailedBytes",taskGroup.getErrorBytes());
        communication.setLongCounter("writeFailedBytes",0);
        communication.setLongCounter("errorBytes", taskGroup.getErrorBytes());
        communication.setLongCounter("totalErrorBytes",taskGroup.getErrorBytes());

        String errorMessage = taskGroup.getErrorMessage();
        if (StringUtils.isNotBlank(errorMessage)) {
            communication.setThrowable(new Throwable(errorMessage));
        }

        return communication;
    }

}
