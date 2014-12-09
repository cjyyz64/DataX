package com.alibaba.datax.core.manager.impl;

import com.alibaba.datax.core.manager.DataServiceManager;
import com.alibaba.datax.core.util.HttpClientUtil;
import com.alibaba.datax.core.util.SerializationUtil;
import com.alibaba.datax.service.face.domain.Result;
import com.alibaba.datax.service.face.domain.TaskGroup;
import com.alibaba.datax.service.face.domain.TaskGroupStatus;
import com.google.gson.reflect.TypeToken;
import com.jayway.restassured.response.Response;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;

import static com.jayway.restassured.RestAssured.given;


/**
 * Created by hongjiao.hj on 2014/12/9.
 */
public class DataServiceManagerImpl implements DataServiceManager{

    private static final String prefixUrl = "http://datax-service/";

    private static HttpClientUtil httpClientUtil = HttpClientUtil.getHttpClientUtil();

    @Override
    public Result<?> getJobInfo(Long jobId) {
        String url = prefixUrl + "inner/job/" + jobId + "/state";
        System.out.println("getJobInfo url: " + url);

        try {
            HttpGet httpGet = HttpClientUtil.getGetRequest();
            httpGet.setURI(new URI(url));
            httpClientUtil.executeAndGetWithRetry(httpGet,3,1000l);
            //todo： json转对象,依赖ds的job对象
            return null;

        } catch (Exception e) {
            System.err.println("getJobInfo error");
            throw new RuntimeException("getJobInfo error");
        }
    }

    @Override
    public Result<Boolean> updateJobInfo(Long jobId, String jobObject) {
        //todo:   依赖ds的job对象
        return null;
    }

    @Override
    public Result<List<TaskGroup>> getTaskGroupInJob(Long jobId) {
        String url = prefixUrl + "inner/job/" + jobId + "/taskGroup/status";

//        Response response = given()
//        .when().get(url);
//        String jsonStr = response.getBody().asString();
//        Type type = new TypeToken<Result<List<TaskGroup>>>(){}.getType();
//        Result<List<TaskGroup>> result = SerializationUtil.gson2Object(jsonStr,type);

        try {
            HttpGet httpGet = HttpClientUtil.getGetRequest();
            httpGet.setURI(new URI(url));

            String resJson = httpClientUtil.executeAndGetWithRetry(httpGet,3,1000l);

            Type type = new TypeToken<Result<List<TaskGroup>>>(){}.getType();
            Result<List<TaskGroup>> result = SerializationUtil.gson2Object(resJson,type);
            return result;
        } catch (Exception e) {
            System.err.println("getJobInfo error");
            throw new RuntimeException("getTaskGroupInJob error");
        }
    }

    @Override
    public Result<Boolean> startTaskGroup(Long jobId, TaskGroup taskGroup) {
        String url = prefixUrl + "inner/job/" + jobId + "/taskGroup";
        try {
            HttpPost httpPost = HttpClientUtil.getPostRequest();
            httpPost.setURI(new URI(url));

            StringEntity jsonEntity = new StringEntity(SerializationUtil.gson2String(taskGroup));
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPost.setEntity(jsonEntity);

            String resJson = httpClientUtil.executeAndGetWithRetry(httpPost,3,1000l);
            Type type = new TypeToken<Result<Boolean>>(){}.getType();
            Result<Boolean> result = SerializationUtil.gson2Object(resJson,type);
            return result;
//            Response response = given()
//                    .body(SerializationUtil.gson2String(taskGroup))
//                    .when().post(url);
//            return null;
        } catch(Exception e) {
            System.err.println("startTaskGroup error, groupId = " + taskGroup.getTaskGroupId());
            throw new RuntimeException("startTaskGroup error");
        }
    }

    @Override
    public Result<Boolean> killTaskGroup(Long jobId, Long taskGroupId) {
        String url = prefixUrl + "inner/job/" + jobId + "/taskGroup/" + taskGroupId;
        try {
            HttpDelete httpDelete = HttpClientUtil.getDeleteRequest();
            httpDelete.setURI(new URI(url));

            String resJson = httpClientUtil.executeAndGetWithRetry(httpDelete,3,1000l);

            Type type = new TypeToken<Result<Boolean>>(){}.getType();
            Result<Boolean> result = SerializationUtil.gson2Object(resJson,type);
            return result;

        } catch (Exception e) {
            System.err.println("killTaskGroup error");
            throw new RuntimeException("killTaskGroup error");
        }
    }

    @Override
    public Result<Boolean> updateTaskGroupInfo(Long jobId, Long taskGroupId, TaskGroupStatus taskGroupStatus) {
        String url = prefixUrl + "inner/job/" + jobId + "/taskGroup/" + taskGroupId;
        try {
            HttpPut httpPut = HttpClientUtil.getPutRequest();
            httpPut.setURI(new URI(url));


            StringEntity jsonEntity = new StringEntity(SerializationUtil.gson2String(taskGroupStatus));
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);

            String resJson = httpClientUtil.executeAndGetWithRetry(httpPut,3,1000l);
            Type type = new TypeToken<Result<Boolean>>(){}.getType();
            Result<Boolean> result = SerializationUtil.gson2Object(resJson,type);
            return result;
        } catch (Exception e) {
            System.err.println("updateTaskGroupInfo error");
            throw new RuntimeException("updateTaskGroupInfo error");
        }
    }
}
