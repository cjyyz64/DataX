package com.starrocks.connector.datax.plugin.writer.starrockswriter.manager;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.alibaba.fastjson.JSON;
import com.starrocks.connector.datax.plugin.writer.starrockswriter.StarRocksWriterOptions;
import com.starrocks.connector.datax.plugin.writer.starrockswriter.row.StarRocksDelimiterParser;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

 
public class StarRocksStreamLoadVisitor {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksStreamLoadVisitor.class);

    private final StarRocksWriterOptions writerOptions;
    private int pos;

    public StarRocksStreamLoadVisitor(StarRocksWriterOptions writerOptions) {
        this.writerOptions = writerOptions;
    }

    public void doStreamLoad(StarRocksFlushTuple flushData) throws IOException {
        String host = getAvailableHost();
        if (null == host) {
            throw new IOException("None of the host in `load_url` could be connected.");
        }
        String loadUrl = new StringBuilder(host)
            .append("/api/")
            .append(writerOptions.getDatabase())
            .append("/")
            .append(writerOptions.getTable())
            .append("/_stream_load")
            .toString();
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Start to join batch data: rows[%d] bytes[%d] label[%s].", flushData.getRows().size(), flushData.getBytes(), flushData.getLabel()));
        }
        Map<String, Object> loadResult = doHttpPut(loadUrl, flushData.getLabel(), joinRows(flushData.getRows(), flushData.getBytes().intValue()));
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            LOG.error("unknown result status. {}", loadResult);
            throw new IOException("Unable to flush data to StarRocks: unknown result status. " + loadResult);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(new StringBuilder("StreamLoad response:\n").append(JSON.toJSONString(loadResult)).toString());
        }
        if ("Fail".equals(loadResult.get(keyStatus))) {
            StringBuilder errorBuilder = new StringBuilder("Failed to flush data to StarRocks.\n");
            if (loadResult.containsKey("Message")) {
                errorBuilder.append(loadResult.get("Message"));
                errorBuilder.append('\n');
            }
            if (loadResult.containsKey("ErrorURL")) {
                LOG.error("StreamLoad response: {}", loadResult);
                try {
                    errorBuilder.append(doHttpGet(loadResult.get("ErrorURL").toString()));
                    errorBuilder.append('\n');
                } catch (IOException e) {
                    LOG.warn("Get Error URL failed. {} ", loadResult.get("ErrorURL"), e);
                }
            } else {
                errorBuilder.append(JSON.toJSONString(loadResult));
                errorBuilder.append('\n');
            }
            throw new IOException(errorBuilder.toString());
        }
    }

    private String getAvailableHost() {
        List<String> hostList = writerOptions.getLoadUrlList();
        if (pos >= hostList.size()) {
            pos = 0;
        }
        for (; pos < hostList.size(); pos++) {
            String host = new StringBuilder("http://").append(hostList.get(pos)).toString();
            if (tryHttpConnection(host)) {
                return host;
            }
        }
        return null;
    }

    private boolean tryHttpConnection(String host) {
        try {  
            URL url = new URL(host);
            HttpURLConnection co =  (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(1000);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e1) {
            LOG.warn("Failed to connect to address:{}", host, e1);
            return false;
        }
    }

    private byte[] joinRows(List<byte[]> rows, int totalBytes) {
        if (StarRocksWriterOptions.StreamLoadFormat.CSV.equals(writerOptions.getStreamLoadFormat())) {
            Map<String, Object> props = writerOptions.getLoadProps();
            byte[] lineDelimiter = StarRocksDelimiterParser.parse((String)props.get("row_delimiter"), "\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + rows.size() * lineDelimiter.length);
            for (byte[] row : rows) {
                bos.put(row);
                bos.put(lineDelimiter);
            }
            return bos.array();
        }
       
        if (StarRocksWriterOptions.StreamLoadFormat.JSON.equals(writerOptions.getStreamLoadFormat())) {
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + (rows.isEmpty() ? 2 : rows.size() + 1));
            bos.put("[".getBytes(StandardCharsets.UTF_8));
            byte[] jsonDelimiter = ",".getBytes(StandardCharsets.UTF_8);
            boolean isFirstElement = true;
            for (byte[] row : rows) {
                if (!isFirstElement) {
                    bos.put(jsonDelimiter);
                }
                bos.put(row);
                isFirstElement = false;
            }
            bos.put("]".getBytes(StandardCharsets.UTF_8));
            return bos.array();
        }
        throw new RuntimeException("Failed to join rows data, unsupported `format` from stream load properties:");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> doHttpPut(String loadUrl, String label, byte[] data) throws IOException {
        LOG.info(String.format("Executing stream load to: '%s', size: '%s'", loadUrl, data.length));
        try (CloseableHttpClient httpclient = buildHttpClient()) {
            HttpPut httpPut = new HttpPut(loadUrl);
            List<String> cols = writerOptions.getColumns();
            if (null != cols && !cols.isEmpty()) {
                httpPut.setHeader("columns", String.join(",", cols));
            }
            if (null != writerOptions.getLoadProps()) {
                for (Map.Entry<String, Object> entry : writerOptions.getLoadProps().entrySet()) {
                    httpPut.setHeader(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }
            httpPut.setHeader("Expect", "100-continue");
            httpPut.setHeader("label", label);
            httpPut.setHeader("Content-Type", "application/x-www-form-urlencoded");
            httpPut.setHeader("Authorization", getBasicAuthHeader(writerOptions.getUsername(), writerOptions.getPassword()));
            httpPut.setEntity(new ByteArrayEntity(data));
            httpPut.setConfig(RequestConfig.custom().setRedirectsEnabled(true).build());
            try (CloseableHttpResponse resp = httpclient.execute(httpPut)) {
                int code = resp.getStatusLine().getStatusCode();
                if (200 != code) {
                    String errorText;
                    try {
                        HttpEntity respEntity = resp.getEntity();
                        errorText = EntityUtils.toString(respEntity);
                    } catch (Exception err) {
                        errorText = "find errorText failed: " + err.getMessage();
                    }
                    LOG.warn("Request failed with code:{}, err:{}", code, errorText);
                    Map<String, Object> errorMap = new HashMap<>();
                    errorMap.put("Status", "Fail");
                    errorMap.put("Message", errorText);
                    return errorMap;
                }
                HttpEntity respEntity = resp.getEntity();
                if (null == respEntity) {
                    LOG.warn("Request failed with empty response.");
                    return null;
                }
                return (Map<String, Object>)JSON.parse(EntityUtils.toString(respEntity));
            }
        }
    }

    private String doHttpGet(String getUrl) throws IOException {
        LOG.info("Executing GET from {}.", getUrl);
        try (CloseableHttpClient httpclient = buildHttpClient()) {
            HttpGet httpGet = new HttpGet(getUrl);
            try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                HttpEntity respEntity = resp.getEntity();
                if (null == respEntity) {
                    LOG.warn("Request failed with empty response.");
                    return null;
                }
                return EntityUtils.toString(respEntity);
            }
        }
    }

    private CloseableHttpClient buildHttpClient(){
        final HttpClientBuilder httpClientBuilder = HttpClients.custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });
        return httpClientBuilder.build();
    }
    
    private String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
    }

}
