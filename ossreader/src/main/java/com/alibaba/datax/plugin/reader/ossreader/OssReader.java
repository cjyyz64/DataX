package com.alibaba.datax.plugin.reader.ossreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.ossreader.util.OssUtil;
import com.alibaba.datax.plugin.unstructuredstorage.Constant;
import com.alibaba.datax.plugin.unstructuredstorage.UnstructuredStorageReaderUtil;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by mengxin.liumx on 2014/12/7.
 */
public class OssReader extends Reader {
    public static class Master extends Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(OssReader.Master.class);

        private Configuration readerOriginConfig = null;

        @Override
        public void init() {
            LOG.debug("init() begin...");
            this.readerOriginConfig = this.getPluginJobConf();
            this.validate();
            LOG.debug("init() ok and end...");
        }

        // TODO column 校验
        private void validate() {
            String charset = this.readerOriginConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.Key.CHARSET,
                    Constant.DEFAULT_CHARSET);
            try {
                Charsets.toCharset(charset);
            } catch (UnsupportedCharsetException uce) {
                throw DataXException.asDataXException(
                        OssReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        String.format("不支持的编码格式 : [%s]", charset), uce);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        OssReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        String.format("运行配置异常 : %s", e.getMessage()), e);
            }

        }

        @Override
        public void prepare() {
            LOG.debug("prepare()");
        }

        @Override
        public void post() {
            LOG.debug("post()");
        }

        @Override
        public void destroy() {
            LOG.debug("destroy()");
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

            // 将每个单独的 object 作为一个 slice
            List<String> objects = parseOriginObjects(readerOriginConfig.getList(Constants.OBJECT, String.class));

            for (String object : objects) {
                Configuration splitedConfig = this.readerOriginConfig.clone();
                splitedConfig.set(Constants.OBJECT, object);
                readerSplitConfigs.add(splitedConfig);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        private List<String> parseOriginObjects(List<String> originObjects) {
            List<String> parsedObjects = new ArrayList<String>();

            for (String object : originObjects){
                int firstMetaChar = (object.indexOf('*') > object.indexOf('?')) ?
                        object.indexOf('*') : object.indexOf('?');

                if (firstMetaChar != -1){
                    int lastDirSeparator = object.lastIndexOf(IOUtils.DIR_SEPARATOR, firstMetaChar);
                    String parentDir = object.substring(0, lastDirSeparator + 1);
                    List<String> remoteObjects = getRemoteObjects(parentDir);
                    Pattern pattern = Pattern.compile(object.replace("*", ".*").replace("?", ".?"));

                    for (String remoteObject : remoteObjects){
                        if (pattern.matcher(remoteObject).matches()){
                            parsedObjects.add(remoteObject);
                        }
                    }
                } else {
                    parsedObjects.add(object);
                }
            }
            return parsedObjects;
        }

        // TODO OSS异常校验
        private List<String> getRemoteObjects(String parentDir) throws OSSException,ClientException{

            List<String> remoteObjects = new ArrayList<String>();
            OSSClient client = OssUtil.initOssClient(readerOriginConfig);

            ListObjectsRequest listObjectsRequest= new ListObjectsRequest(readerOriginConfig.getString(Key.BUCKET));
            listObjectsRequest.setPrefix(parentDir);
            ObjectListing objectList;
            do {
                objectList = client.listObjects(listObjectsRequest);
                for (OSSObjectSummary objectSummary : objectList.getObjectSummaries()){
                    remoteObjects.add(objectSummary.getKey());
                }
                listObjectsRequest.setMarker(objectList.getNextMarker());

            } while (!objectList.isTruncated());

            return remoteObjects;
        }

    }

    public static class Slave extends Task{
        private static Logger LOG = LoggerFactory.getLogger(Slave.class);

        private Configuration readerSliceConfig;


        // TODO OSS异常校验
        @Override
        public void startRead(RecordSender recordSender) {
            LOG.debug("read start");
            String object = readerSliceConfig.getString(Key.OBJECT);
            OSSClient client = OssUtil.initOssClient(readerSliceConfig);
            // TODO 改为 request 模式
            OSSObject ossObject = client.getObject(readerSliceConfig.getString(Key.BUCKET), object);
            InputStream objectStream = ossObject.getObjectContent();
            UnstructuredStorageReaderUtil.readFromStream(objectStream,
                    object, this.readerSliceConfig, recordSender,
                    this.getTaskPluginCollector());
            recordSender.flush();
        }

        @Override
        public void init() {
            this.readerSliceConfig = this.getPluginJobConf();
        }

        @Override
        public void destroy() {

        }
    }
}
