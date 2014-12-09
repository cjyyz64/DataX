package com.alibaba.datax.core.util;

import com.alibaba.datax.service.face.domain.Result;
import com.alibaba.datax.service.face.domain.TaskGroup;
import com.alibaba.datax.service.face.domain.TaskGroupStatus;
import com.google.gson.reflect.TypeToken;
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

    public static Result<?> getJobInfo(Long jobId) {
        String url = basicUrl + "inner/job/" + jobId + "/state";
        System.out.println("getJobInfo url: " + url);

        try {
            HttpGet httpGet = HttpClientUtil.getGetRequest();
            httpGet.setURI(new URI(url));
            httpClientUtil.executeAndGetWithRetry(httpGet, 3, 1000l);
            //todo： json转对象,依赖ds的job对象
            return null;

        } catch (Exception e) {
            System.err.println("getJobInfo error");
            throw new RuntimeException("getJobInfo error");
        }
    }

    public static Result<Boolean> updateJobInfo(Long jobId, String jobObject) {
        //todo:   依赖ds的job对象
        return null;
    }

    public static Result<List<TaskGroup>> getTaskGroupInJob(Long jobId) {
        String url = basicUrl + "inner/job/" + jobId + "/taskGroup/status";

//        Response response = given()
//        .when().get(url);
//        String jsonStr = response.getBody().asString();
//        Type type = new TypeToken<Result<List<TaskGroup>>>(){}.getType();
//        Result<List<TaskGroup>> result = SerializationUtil.gson2Object(jsonStr,type);

        try {
            HttpGet httpGet = HttpClientUtil.getGetRequest();
            httpGet.setURI(new URI(url));

            String resJson = httpClientUtil.executeAndGetWithRetry(httpGet, 3, 1000l);

            Type type = new TypeToken<Result<List<TaskGroup>>>() {
            }.getType();
            Result<List<TaskGroup>> result = SerializationUtil.gson2Object(resJson, type);
            return result;
        } catch (Exception e) {
            System.err.println("getJobInfo error");
            throw new RuntimeException("getTaskGroupInJob error");
        }
    }

    public static Result<Boolean> startTaskGroup(Long jobId, TaskGroup taskGroup) {
        String url = basicUrl + "inner/job/" + jobId + "/taskGroup";
        try {
            HttpPost httpPost = HttpClientUtil.getPostRequest();
            httpPost.setURI(new URI(url));

            StringEntity jsonEntity = new StringEntity(SerializationUtil.gson2String(taskGroup));
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPost.setEntity(jsonEntity);

            String resJson = httpClientUtil.executeAndGetWithRetry(httpPost, 3, 1000l);
            Type type = new TypeToken<Result<Boolean>>() {
            }.getType();
            Result<Boolean> result = SerializationUtil.gson2Object(resJson, type);
            return result;
//            Response response = given()
//                    .body(SerializationUtil.gson2String(taskGroup))
//                    .when().post(url);
//            return null;
        } catch (Exception e) {
            System.err.println("startTaskGroup error, groupId = " + taskGroup.getTaskGroupId());
            throw new RuntimeException("startTaskGroup error");
        }
    }

    public static Result<Boolean> killTaskGroup(Long jobId, Integer taskGroupId) {
        String url = basicUrl + "inner/job/" + jobId + "/taskGroup/" + taskGroupId;
        try {
            HttpDelete httpDelete = HttpClientUtil.getDeleteRequest();
            httpDelete.setURI(new URI(url));

            String resJson = httpClientUtil.executeAndGetWithRetry(httpDelete, 3, 1000l);

            Type type = new TypeToken<Result<Boolean>>() {
            }.getType();
            Result<Boolean> result = SerializationUtil.gson2Object(resJson, type);
            return result;

        } catch (Exception e) {
            System.err.println("killTaskGroup error");
            throw new RuntimeException("killTaskGroup error");
        }
    }

    public static Result<Boolean> updateTaskGroupInfo(Long jobId, Long taskGroupId, TaskGroupStatus taskGroupStatus) {
        String url = basicUrl + "inner/job/" + jobId + "/taskGroup/" + taskGroupId;
        try {
            HttpPut httpPut = HttpClientUtil.getPutRequest();
            httpPut.setURI(new URI(url));


            StringEntity jsonEntity = new StringEntity(SerializationUtil.gson2String(taskGroupStatus));
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);

            String resJson = httpClientUtil.executeAndGetWithRetry(httpPut, 3, 1000l);
            Type type = new TypeToken<Result<Boolean>>() {
            }.getType();
            Result<Boolean> result = SerializationUtil.gson2Object(resJson, type);
            return result;
        } catch (Exception e) {
            System.err.println("updateTaskGroupInfo error");
            throw new RuntimeException("updateTaskGroupInfo error");
        }
    }
}
