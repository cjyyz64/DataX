package com.alibaba.datax.core.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;

import static org.mockito.Matchers.any;

/**
 * Created by hongjiao.hj on 2014/12/22.
 */
public class HttpClientUtilTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientUtilTest.class);

    @Test
    public void testExecuteAndGet() throws Exception {
        HttpGet httpGet = HttpClientUtil.getGetRequest();
        httpGet.setURI(new URI("http://127.0.0.1:8080"));

        CloseableHttpClient httpClient = PowerMockito.mock(CloseableHttpClient.class);
        CloseableHttpResponse response = PowerMockito.mock(CloseableHttpResponse.class);

        StatusLine statusLine = PowerMockito.mock(StatusLine.class);
        PowerMockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_BAD_REQUEST);
        PowerMockito.when(response.getStatusLine()).thenReturn(statusLine);
        PowerMockito.when(httpClient.execute(any(HttpRequestBase.class))).thenReturn(response);

        try {
            HttpClientUtil client = HttpClientUtil.getHttpClientUtil();
            ReflectUtil.setField(client, "httpClient", httpClient);
            client.executeAndGet(httpGet);
        } catch (Exception e) {
            LOGGER.info("msg:" + e.getMessage(), e);
            Assert.assertNotNull(e);
            Assert.assertEquals("Response Status Code : 400", e.getMessage());

        }

        try {
            PowerMockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
            PowerMockito.when(response.getEntity()).thenReturn(null);
            HttpClientUtil client = HttpClientUtil.getHttpClientUtil();
            ReflectUtil.setField(client, "httpClient", httpClient);
            client.executeAndGet(httpGet);
        } catch (Exception e) {
            LOGGER.info("msg:" + e.getMessage(), e);
            Assert.assertNotNull(e);
            Assert.assertEquals("Response Entity Is Null", e.getMessage());

        }

        InputStream is = new ByteArrayInputStream("abc".getBytes());
        HttpEntity entity = PowerMockito.mock(HttpEntity.class);
        PowerMockito.when(response.getEntity()).thenReturn(entity);
        PowerMockito.when(entity.getContent()).thenReturn(is);
        HttpClientUtil client = HttpClientUtil.getHttpClientUtil();
        ReflectUtil.setField(client, "httpClient", httpClient);
        String result = client.executeAndGet(httpGet);
        Assert.assertEquals("abc", result);
        LOGGER.info("result:" + result);

    }



    @Test
    public void testExecuteAndGetWithRetry() throws Exception {
        String url = "http://127.0.0.1/:8080";
        HttpRequestBase httpRequestBase = new HttpGet(url);

        HttpClientUtil httpClientUtil = PowerMockito.spy(HttpClientUtil.getHttpClientUtil());


        PowerMockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                System.out.println("失败第1次");
                return new Exception("失败第1次");
            }
        }).doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                System.out.println("失败第2次");
                return new Exception("失败第2次");
            }
        }).doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                System.out.println("失败第3次");
                return new Exception("失败第3次");
            }
        })
                .doReturn("成功")
                .when(httpClientUtil).executeAndGet(any(HttpRequestBase.class));


       String str =  httpClientUtil.executeAndGetWithRetry(httpRequestBase,3,100l);
       Assert.assertEquals(str,"成功");

    }

}
