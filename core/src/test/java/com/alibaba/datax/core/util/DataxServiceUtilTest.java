package com.alibaba.datax.core.util;

import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.LogReportInfo;
import com.alibaba.fastjson.JSON;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.junit.Test;

import java.net.URI;

/**
 * Created by liqiang on 15/12/24.
 */
public class DataxServiceUtilTest {

    @Test
    public void testPool() throws Exception {

        CoreConstant.DATAX_SECRET_PATH="/Users/liqiang/1_AliGit/datax/core/src/main/conf/.secret.properties";

        //DataxServiceUtil.init("http://100.69.165.189:7001/dsc/",1000);
        String url_error = "http://100.69.165.189:7001/dsc/inner/job/reportDataxLog1";
        String url_right = "http://100.69.165.189:7001/dsc/inner/job/reportDataxLog";
        LogReportInfo info = new LogReportInfo();
        HttpClientUtil.setHttpTimeoutInMillionSeconds(1000);
        HttpClientUtil httpClientUtil = HttpClientUtil.getHttpClientUtil();

        for(int i=0;i<31;i++) {
            try {
                HttpPut httpPut = HttpClientUtil.getPutRequest();
                httpPut.setURI(new URI(url_error));

                StringEntity jsonEntity = new StringEntity(JSON.toJSONString(info), "UTF-8");
                jsonEntity.setContentEncoding("UTF-8");
                jsonEntity.setContentType("application/json");
                httpPut.setEntity(jsonEntity);
                DataxServiceUtil.signature(url_error, "PUT", httpPut, JSON.toJSONString(info));

                String resJson = httpClientUtil.executeAndGet(httpPut);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
            }
        }

        for(int i=0;i<31;i++) {
            try {
                HttpPut httpPut = HttpClientUtil.getPutRequest();
                httpPut.setURI(new URI(url_right));

                StringEntity jsonEntity = new StringEntity(JSON.toJSONString(info), "UTF-8");
                jsonEntity.setContentEncoding("UTF-8");
                jsonEntity.setContentType("application/json");
                httpPut.setEntity(jsonEntity);
                DataxServiceUtil.signature(url_right, "PUT", httpPut, JSON.toJSONString(info));

                String resJson = httpClientUtil.executeAndGet(httpPut);
                System.out.println(resJson);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
            }
        }

        try {
            HttpPut httpPut = HttpClientUtil.getPutRequest();
            httpPut.setURI(new URI(url_error));

            StringEntity jsonEntity = new StringEntity(JSON.toJSONString(info), "UTF-8");
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);
            DataxServiceUtil.signature(url_right, "PUT", httpPut, JSON.toJSONString(info));

            String resJson = httpClientUtil.executeAndGetWithRetry(httpPut, 10, 1);
            System.out.println(resJson);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }

        try {
            HttpPut httpPut = HttpClientUtil.getPutRequest();
            httpPut.setURI(new URI(url_right));

            StringEntity jsonEntity = new StringEntity(JSON.toJSONString(info), "UTF-8");
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);
            DataxServiceUtil.signature(url_right, "PUT", httpPut, JSON.toJSONString(info));

            String resJson = httpClientUtil.executeAndGetWithRetry(httpPut,10,1);
            System.out.println(resJson);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }


        try {
            HttpPut httpPut = HttpClientUtil.getPutRequest();
            httpPut.setURI(new URI(url_error));

            StringEntity jsonEntity = new StringEntity(JSON.toJSONString(info), "UTF-8");
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);
            DataxServiceUtil.signature(url_right, "PUT", httpPut, JSON.toJSONString(info));

            String resJson = httpClientUtil.executeAndGetWithFailedRetry(httpPut, 10, 1);
            System.out.println(resJson);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }

        try {
            HttpPut httpPut = HttpClientUtil.getPutRequest();
            httpPut.setURI(new URI(url_right));

            StringEntity jsonEntity = new StringEntity(JSON.toJSONString(info), "UTF-8");
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);
            DataxServiceUtil.signature(url_right, "PUT", httpPut, JSON.toJSONString(info));

            String resJson = httpClientUtil.executeAndGetWithFailedRetry(httpPut,10,1);
            System.out.println(resJson);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }

    }
}