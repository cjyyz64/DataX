package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RetryUtil;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.concurrent.Callable;

public class HttpClientUtil {

    private CloseableHttpClient httpClient;

    private volatile static HttpClientUtil clientUtil;

    //构建httpclient的时候一定要设置这两个参数。淘宝很多生产故障都由此引起
    private static int HTTP_TIMEOUT_INMILLIONSECONDS;

    private final int POOL_SIZE = 20;

    public static void setHttpTimeoutInMillionSeconds(int httpTimeoutInMillionSeconds) {
        HTTP_TIMEOUT_INMILLIONSECONDS = httpTimeoutInMillionSeconds;
    }

    public static HttpClientUtil getHttpClientUtil() {
        if (null == clientUtil) {
            synchronized (HttpClientUtil.class) {
                if (null == clientUtil) {
                    clientUtil = new HttpClientUtil();
                }
            }
        }
        return clientUtil;
    }

    private HttpClientUtil() {
        initApacheHttpClient();
    }

    public void destroy() {
        destroyApacheHttpClient();
    }

    // 创建包含connection pool与超时设置的client
    private void initApacheHttpClient() {
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(HTTP_TIMEOUT_INMILLIONSECONDS)
                .setConnectTimeout(HTTP_TIMEOUT_INMILLIONSECONDS).setConnectionRequestTimeout(HTTP_TIMEOUT_INMILLIONSECONDS)
                .setStaleConnectionCheckEnabled(true).build();

        CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials("datax","datax"));

        httpClient = HttpClientBuilder.create().setMaxConnTotal(POOL_SIZE).setMaxConnPerRoute(POOL_SIZE)
                .setDefaultRequestConfig(requestConfig).setDefaultCredentialsProvider(provider).build();

    }

    private void destroyApacheHttpClient() {
        try {
            if (httpClient != null) {
                httpClient.close();
                httpClient = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HttpGet getGetRequest() {
        return new HttpGet();
    }

    public static HttpPost getPostRequest() {
        return new HttpPost();
    }

    public static HttpPut getPutRequest() {
        return new HttpPut();
    }

    public static HttpDelete getDeleteRequest() {
        return new HttpDelete();
    }

    public String executeAndGet(HttpRequestBase httpRequestBase) throws Exception {
        HttpResponse response;
        String entiStr = "";
        try {
            response = httpClient.execute(httpRequestBase);

            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                System.err.println("请求地址：" + httpRequestBase.getURI() + ", 请求方法：" + httpRequestBase.getMethod()
                        + ",STATUS CODE = " + response.getStatusLine().getStatusCode());

                throw new Exception("Response Status Code : " + response.getStatusLine().getStatusCode());
            } else {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    entiStr = EntityUtils.toString(entity, Consts.UTF_8);
                } else {
                    throw new Exception("Response Entity Is Null");
                }
            }
        } catch (Exception e) {
            throw e;
        }

        return entiStr;
    }

    public String executeAndGetWithRetry(final HttpRequestBase httpRequestBase, final int retryTimes, final long retryInterval) {
        try {
            return RetryUtil.executeWithRetry(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return executeAndGet(httpRequestBase);
                }
            }, retryTimes, retryInterval, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }

}
