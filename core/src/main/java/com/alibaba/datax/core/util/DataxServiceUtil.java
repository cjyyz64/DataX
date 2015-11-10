package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.JobStatusDto;
import com.alibaba.datax.dataxservice.face.domain.LogReportInfo;
import com.alibaba.datax.dataxservice.face.domain.Result;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupDto;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupStatusDto;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpMessage;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;


public final class DataxServiceUtil {
    private static final Logger logger = LoggerFactory
            .getLogger(DataxServiceUtil.class);
    private static final String SIGNATURE_METHOD_HMAC_SHA1 = "Hmac-SHA1";               // 签名算法
    private static final String ENCODE_TYPE                = "UTF-8";                   // 编码
    private static final String SEPARATOR                  = "&";                       //
    private static final String EQUAL                      = "=";                       //
    private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    
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
            DataxServiceUtil.signature(url, "GET", httpGet, null);
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

    public static Result updateJobInfo(Long jobId, JobStatusDto jobStatus) {
        String url = DATAX_SERVICE_URL + "inner/job/" + jobId + "/status";
        try {
            HttpPut httpPut = HttpClientUtil.getPutRequest();
            httpPut.setURI(new URI(url));

            StringEntity jsonEntity = new StringEntity(JSON.toJSONString(jobStatus), "UTF-8");
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);
            DataxServiceUtil.signature(url, "PUT", httpPut, JSON.toJSONString(jobStatus));

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

    public static Result<List<TaskGroupDto>> getTaskGroupInJob(Long jobId) {
        String url = DATAX_SERVICE_URL + "inner/job/" + jobId + "/taskGroup";

        try {
            HttpGet httpGet = HttpClientUtil.getGetRequest();
            httpGet.setURI(new URI(url));
            DataxServiceUtil.signature(url, "GET", httpGet, null);
            String resJson = httpClientUtil.executeAndGetWithRetry(httpGet, 9, 1000l);

            Result<List<TaskGroupDto>> result = JSON.parseObject(resJson,
                    new TypeReference<Result<List<TaskGroupDto>>>(){});

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

    public static Result<List<TaskGroupStatusDto>> getTaskGroupStatusInJob(Long jobId) {
        String url = DATAX_SERVICE_URL + "inner/job/" + jobId + "/taskGroup/status";

        try {
            HttpGet httpGet = HttpClientUtil.getGetRequest();
            httpGet.setURI(new URI(url));
            DataxServiceUtil.signature(url, "GET", httpGet, null);
            String resJson = httpClientUtil.executeAndGetWithRetry(httpGet, 9, 1000l);

            Result<List<TaskGroupStatusDto>> result = JSON.parseObject(resJson,
                    new TypeReference<Result<List<TaskGroupStatusDto>>>(){});

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

    public static Result startTaskGroup(Long jobId, TaskGroupDto taskGroup) {
        String url = DATAX_SERVICE_URL + "inner/job/" + jobId + "/taskGroup";
        try {
            HttpPost httpPost = HttpClientUtil.getPostRequest();
            httpPost.setURI(new URI(url));

            StringEntity jsonEntity = new StringEntity(JSON.toJSONString(taskGroup), "UTF-8");
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPost.setEntity(jsonEntity);
            DataxServiceUtil.signature(url, "POST", httpPost, JSON.toJSONString(taskGroup));

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
            DataxServiceUtil.signature(url, "DELETE", httpDelete, null);

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

    public static Result updateTaskGroupInfo(Long jobId, Integer taskGroupId, TaskGroupStatusDto taskGroupStatus) {
        String url = DATAX_SERVICE_URL + "inner/job/" + jobId + "/taskGroup/" + taskGroupId + "/status";
        try {
            HttpPut httpPut = HttpClientUtil.getPutRequest();
            httpPut.setURI(new URI(url));


            StringEntity jsonEntity = new StringEntity(JSON.toJSONString(taskGroupStatus), "UTF-8");
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);
            DataxServiceUtil.signature(url, "PUT", httpPut, JSON.toJSONString(taskGroupStatus));

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

    public static Result reportDataxLog(LogReportInfo info){
        String url = DATAX_SERVICE_URL + "/inner/job/reportDataxLog";
        try {
            HttpPut httpPut = HttpClientUtil.getPutRequest();
            httpPut.setURI(new URI(url));

            StringEntity jsonEntity = new StringEntity(JSON.toJSONString(info), "UTF-8");
            jsonEntity.setContentEncoding("UTF-8");
            jsonEntity.setContentType("application/json");
            httpPut.setEntity(jsonEntity);
            DataxServiceUtil.signature(url, "PUT", httpPut, JSON.toJSONString(info));
            
            String resJson = httpClientUtil.executeAndGet(httpPut);

            Type type = new TypeReference<Result<Object>>() {}.getType();
            Result result = JSON.parseObject(resJson,type);

            if (!result.isSuccess()) {
                throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED,
                        String.format("logReportInfo fail, http result:[%s].", resJson));
            }

            return result;
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.CALL_DATAX_SERVICE_FAILED, e);
        }
    }


    public static Communication convertTaskGroupToCommunication(TaskGroupStatusDto taskGroupStatus) {
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

        //todo 等待datax-service-face 1.0.4的mvn
//        if(taskGroupStatus.getWaitReaderCount() == null){
//            taskGroupStatus.setWaitReaderCount(0L);
//        }
//        if(taskGroupStatus.getWaitWriterCount() == null){
//            taskGroupStatus.setWaitWriterCount(0L);
//        }
//
//        communication.setLongCounter(CommunicationTool.WAIT_READER_TIME, taskGroupStatus.getWaitReaderCount());
//        communication.setLongCounter(CommunicationTool.WAIT_WRITER_TIME, taskGroupStatus.getWaitWriterCount());
        //

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
    
    
    public static void signature(String url, String method,
            HttpMessage httpMessage, String body) {
        Properties properties = SecretUtil.getSecurityProperties();
        String currentKeyVersion = properties
                .getProperty(CoreConstant.CURRENT_KEYVERSION);
        String currentKeyContent = properties
                .getProperty(CoreConstant.CURRENT_KEYCONTENT);
        String lastKeyVersion = properties
                .getProperty(CoreConstant.LAST_KEYVERSION);
        String lastKeyContent = properties
                .getProperty(CoreConstant.LAST_KEYCONTENT);

        boolean needSignature = false;
        String signatureId = "";
        String signatureKey = "";
        if (StringUtils.isNotBlank(currentKeyVersion)
                && StringUtils.isNotBlank(currentKeyContent)) {
            signatureId = currentKeyVersion;
            signatureKey = currentKeyContent;
            needSignature = true;
        } else if (StringUtils.isNotBlank(lastKeyVersion)
                && StringUtils.isNotBlank(lastKeyContent)) {
            signatureId = lastKeyVersion;
            signatureKey = lastKeyContent;
            needSignature = true;
        }

        if (needSignature) {
            Map<String, String> paramsMapToCheck = new HashMap<String, String>();
            String timestamp = DataxServiceUtil.df.format(new Date());
            paramsMapToCheck.put("X-CDP-Timestamp", timestamp);
            httpMessage.addHeader("X-CDP-Timestamp", timestamp);

            method = method.toUpperCase();
            if (method.equals("PUT") || method.equals("POST")) {
                String md5sum = DataxServiceUtil.getMd5(body);
                paramsMapToCheck.put("X-CDP-Contentmd5", md5sum);
                httpMessage.addHeader("X-CDP-Contentmd5", md5sum);
            } else {
                paramsMapToCheck.put("X-CDP-Contentmd5", "");
                httpMessage.addHeader("X-CDP-Contentmd5", "");
            }

            paramsMapToCheck.put("X-CDP-Alisa-Username", signatureId);
            httpMessage.addHeader("X-CDP-Alisa-Username", signatureId);

            try {
                paramsMapToCheck.put("X-CDP-Uri", new URI(url).getPath());
            } catch (URISyntaxException e) {
                throw DataXException
                        .asDataXException(FrameworkErrorCode.SECRET_ERROR,
                                String.format("Illegal url [%s] for signature",
                                        url), e);
            }

            String signatureComputed = DataxServiceUtil.doSignature(
                    paramsMapToCheck, signatureKey, method);
            httpMessage.addHeader("X-CDP-Signature", signatureComputed);
        }

    }

    /**
     * 创建签名
     * 参考：https://docs.aliyun.com/?spm=5176.100054.3.3.UxMq3T#/pub/rds/open-api/call-mode&signature
     * @param parameterMap
     * @param secret
     * @return
     * @throws UnsupportedEncodingException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     */
    private static String doSignature(Map<String, String> parameterMap, String secret, String method){
        List<String> keys = new ArrayList<String>(parameterMap.keySet());
        Collections.sort(keys);
        StringBuilder sb = new StringBuilder();

        try {
            for (String key : keys) {
                String value = parameterMap.get(key);
                sb.append(SEPARATOR).append(percentEncode(key)).append(EQUAL)
                        .append(percentEncode(value));
            }

            StringBuilder signedData = new StringBuilder();
            signedData.append(percentEncode(method));
            signedData.append(SEPARATOR);
            signedData.append(percentEncode("/"));
            signedData.append(SEPARATOR);
            signedData.append(percentEncode(sb.substring(1)));

            SecretKey key = new SecretKeySpec((secret + SEPARATOR).getBytes(ENCODE_TYPE),
                    SIGNATURE_METHOD_HMAC_SHA1);
            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(key);

            return URLEncoder.encode(
                    new String(new Base64().encode(mac.doFinal(signedData.toString().getBytes(
                            ENCODE_TYPE))), ENCODE_TYPE), ENCODE_TYPE);
        } catch (NoSuchAlgorithmException ignore) {
            logger.warn(String.format(
                    "创建签名异常NoSuchAlgorithmException, [%s]",
                    ExceptionTracker.trace(ignore)));
        } catch (InvalidKeyException ignore) {
            logger.warn(String.format("创建签名异常InvalidKeyException, [%s]",
                    ExceptionTracker.trace(ignore)));
        } catch (UnsupportedEncodingException ignore) {
            logger.warn(String.format(
                    "创建签名异常UnsupportedEncodingException, [%s]",
                    ExceptionTracker.trace(ignore)));
        }
        return "";
    }

    /**
     * 其它字符编码
     * @param s
     * @return
     * @throws UnsupportedEncodingException
     */
    private static String percentEncode(String s) throws UnsupportedEncodingException {
        if (s == null)
            return null;
        return URLEncoder.encode(s, ENCODE_TYPE).replace("+", "%20").replace("*", "%2A")
                .replace("%7E", "~");
    }
    
    private static String getMd5(String plainText) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(plainText.getBytes());
            byte b[] = md.digest();
            return URLEncoder.encode(new String(new Base64().encode(b),
                    ENCODE_TYPE), ENCODE_TYPE);
        } catch (Exception e) {
            throw DataXException
                    .asDataXException(
                            FrameworkErrorCode.SECRET_ERROR,
                            String.format("Compute md5sum for http body message error"),
                            e);
        }
    }
}
