package com.alibaba.datax.plugin.writer.zsearchwriter;

import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.core.statistics.plugin.task.util.DirtyRecord;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
    private BlockingDeque<String>          buffer;
    private long                           failedCount;
    private String                         accessId, accessKey;
    private int batchSize, ttl;
    private boolean             gzip;
    private AtomicLong          totalSize;
    private TaskPluginCollector pluginCollector;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public BufferBarrels(ZSearchConfig zSearchConfig, TaskPluginCollector taskPluginCollector) {
        this(zSearchConfig);
        this.pluginCollector = taskPluginCollector;
    }

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
     * DataX还有单行8MB的要求,但是因为我们都是从reader读出的,没有加上太多字符,所以应该不用关心（除非key很长）
     *
     * @param data
     */
    public void addData(Map data) {
        //转成String 方便计算容量
        String dataString = JSON.toJSONString(data);
        //可以多个同时写入,所以用读锁
        Lock readLock = lock.readLock();
        readLock.lock();
        try {
            buffer.add(dataString);
            //计算容量
            totalSize.addAndGet(dataString.getBytes().length);
        } finally {
            readLock.unlock();
        }
        //一定要先释放读锁再刷新,因为发送的时候会用到写锁,如果读锁没释放就会死锁
        tryFlush();
    }

    /**
     * 尝试提交缓冲区数据,同步操作
     */
    private synchronized void tryFlush() {
        if (buffer.size() == batchSize || totalSize.get() > batchSizeLimit) {
            forceFlush();
        }
    }

    /**
     * 手动生成JSON数据,不能用JSON类,因为Buffer中已经转为String,如果用JSON类会出错
     *
     * @return
     */
    public String getJSONData() {
        StringBuilder sb = new StringBuilder("[");
        for (String s : buffer) {
            sb.append(s);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        return sb.toString();
    }

    /**
     * 提交缓冲区数据
     */
    public void forceFlush() {
        if (buffer.size() == 0) {
            return;
        }
        //获得JSON数据,清空缓存,排他切禁写,所以用写锁
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            retrySend(getJSONData(), buffer.size());
            buffer.clear();
            totalSize.set(0);
        } finally {
            writeLock.unlock();
        }

    }

    private void retrySend(final String data, final long length) {
        //网络抖动重试5次,之间不等待
        try {
            RetryUtil.executeWithRetry(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    addBatch(data);
                    return null;
                }
            }, 5, 0, false);
        } catch (Exception e) {
            log.warn("batch insert failed try single");
            //批量失败,转为单条插入
            JSONArray jsonArray=JSONArray.parseArray(data);
            for (Object one : jsonArray) {
                try {
                    JSONObject jsonObject= (JSONObject) one;
                    addSingle(jsonObject.getString("_id"),jsonObject);
                } catch (Exception e1) {
                    if (pluginCollector != null) {
                        //如果仍出错,则进脏数据
                        pluginCollector
                                .collectDirtyRecord(new DirtyRecord(), "insert error: " + one);
                    }
                    failedCount++;
                }
            }
            if (jsonArray.size() != length) {
                log.error("Concurrency Error! currentData:" + data + " length:" + length);
            }
        }
    }

    /**
     * 批量插入
     *
     * @param data
     * @throws Exception
     */
    private void addBatch(String data) throws Exception {
        String url = String.format("%s/%s?alive=%d", baseUrl, accessId, ttl);
        sendToZSearch(url, data);
    }

    /**
     * 单条插入
     *
     * @param data
     * @throws Exception
     */
    private void addSingle(String id,JSONObject data) throws Exception {
        String url = String.format("%s/%s/%s?alive=%d", baseUrl, accessId, id, ttl);
        data.remove("_id");
        sendToZSearch(url, data.toJSONString());
    }

    /**
     * 真实发送方法
     *
     * @param url
     * @param data
     * @throws Exception
     */
    private void sendToZSearch(String url, String data) throws Exception {
        HttpResponse resp = null;
        HttpPost httpPost = null;
        HttpEntity postEntity = null;
        try {
            httpPost = new HttpPost(url);
            httpPost.addHeader("Connection", "Keep-Alive");
            httpPost.addHeader("token", accessKey);
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
                throw new RuntimeException("Batch insert Error: " + result);
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
