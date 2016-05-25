package com.alibaba.datax.plugin.writer.zsearchwriter;

import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.fastjson.JSONArray;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

/**
 * 数据缓冲桶
 * 收集数据批量发送至ZSearch服务器
 */
public class BufferBarrels {
    private static final Logger log            = LoggerFactory.getLogger(BufferBarrels.class);
    private static final String UTF_8          = "UTF-8";
    private static final long   batchSizeLimit = 64 * 1024 * 1024;

    private String                         baseUrl;
    private PoolingClientConnectionManager cm;
    private HttpClient                     hc;
    private BlockingDeque                  buffer;
    private long                           failedCount;
    private String                         accessId, accessKey;
    private int batchSize, ttl;
    private boolean             gzip;
    private AtomicLong          totalSize;

    //String serverUrl, String accessId, String accessKey, int poolSize, int batchSize
    public BufferBarrels(ZSearchConfig zSearchConfig) {
        this.cm = new PoolingClientConnectionManager();
        this.cm.setMaxTotal(zSearchConfig.httpPoolSize);
        this.cm.setDefaultMaxPerRoute(zSearchConfig.httpPoolSize);
        this.hc = new DefaultHttpClient(cm);
        this.hc.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5000);
        this.hc.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 60000);
        this.hc.getParams().setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 32768);
        this.hc.getParams().setLongParameter(ClientPNames.CONN_MANAGER_TIMEOUT, 500L);

        this.baseUrl = zSearchConfig.endpoint.trim();
        this.batchSize = zSearchConfig.batchSize;
        this.buffer = new LinkedBlockingDeque(batchSize);
        this.accessId = zSearchConfig.accessId;
        this.accessKey = zSearchConfig.accessKey;
        this.gzip = zSearchConfig.gzip;
        this.ttl = zSearchConfig.ttl;
        this.failedCount = 0;
        this.totalSize = new AtomicLong(0);
    }

    /**
     * Gzip 压缩数据
     *
     * @param data
     * @return
     */
    private static byte[] gzip(byte[] data) {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream(10240);
        GZIPOutputStream output = null;
        try {
            output = new GZIPOutputStream(byteOutput);
            output.write(data);
        } catch (IOException e) {
        } finally {
            try {
                output.close();
            } catch (IOException e) {
            }
        }
        return byteOutput.toByteArray();
    }

    /**
     * 添加数据至缓冲区
     * TODO:8MB 限制
     *
     * @param data
     */
    public void addData(Map data) {
        buffer.add(data);
        //计算容量
        totalSize.addAndGet(32 * data.size());
    }

    /**
     * 尝试提交缓冲区数据
     */
    public void tryFlush() {
        if (buffer.size() == batchSize || totalSize.get() > batchSizeLimit) {
            forceFlush();
        }
    }

    public String getJSONData() {
        return JSONArray.toJSONString(buffer);
    }

    /**
     * 提交缓冲区数据
     */
    public void forceFlush() {
        if (buffer.size() == 0) {
            return;
        }
        retrySend(getJSONData(), buffer.size());
        buffer.clear();
        totalSize.set(0);

    }

    private void retrySend(final String data, final long length) {
        //网络抖动重试5次,之间不等待
        try {
            RetryUtil.executeWithRetry(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    sendToZSearch(data);
                    return null;
                }
            }, 5, 0, false);
        } catch (Exception e) {
            log.warn("batch insert error",e);
            failedCount += length;
        }
    }

    private void sendToZSearch(String data) throws Exception {
        HttpResponse resp = null;
        HttpPost httpPost = null;
        HttpEntity postEntity = null;
        try {
            httpPost = new HttpPost(String.format("%s/%s?alive=%d", baseUrl, accessId, ttl));
            httpPost.addHeader("Connection", "Keep-Alive");
            httpPost.addHeader("accessKey", accessKey);
            if (gzip) {
                httpPost.addHeader("Content-Encoding", "gzip");
                byte[] zipData = gzip((data.getBytes(UTF_8)));
                postEntity = new ByteArrayEntity(zipData, ContentType.APPLICATION_JSON);
            } else {
                postEntity = new StringEntity(data, UTF_8);
            }
            httpPost.setEntity(postEntity);
            resp = hc.execute(httpPost);
            String result = EntityUtils.toString(resp.getEntity());
            if (!"OK".equals(result)) {
                throw new RuntimeException("Batch insert Error");
            }
        } finally {
            try {
                if (resp != null) {
                    EntityUtils.consume(resp.getEntity());
                }
                EntityUtils.consume(postEntity);
            } catch (IOException e) {
                log.error("Batch Insert Error", e);
            }
            if (httpPost != null) httpPost.abort();
        }
    }

    /**
     * 获取失败行数
     *
     * @return
     */
    public long getFailedCount() {
        return failedCount;
    }


}
