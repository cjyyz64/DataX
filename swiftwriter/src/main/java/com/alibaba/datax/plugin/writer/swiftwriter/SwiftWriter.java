package com.alibaba.datax.plugin.writer.swiftwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.search.swift.SwiftClient;
import com.alibaba.search.swift.exception.SwiftException;
import com.alibaba.search.swift.protocol.ErrCode;
import com.alibaba.search.swift.protocol.SwiftMessage;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.datax.plugin.writer.swiftwriter.SwiftWriterErrorCode.*;

/**
 * Created by zw86077 on 2015/12/9.
 */
public class SwiftWriter extends Writer {


    public static final class Job extends Writer.Job {
        private final Logger LOG = LoggerFactory.getLogger(this.getClass());

        private Configuration originalConfig;

        //client 连接配置串(必要)
        private String clientConfig;

        //writer config
        private String writerConfig;

        //topic 名字(可选,topic 不存在则创建)
        private String topicName;

        //分区数(可选,topic 创建时使用)
        private int partitionCount;


        private SwiftClient swiftClient;


        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.clientConfig = parseClientConfig(originalConfig);
            this.writerConfig = parseWriterConfig(originalConfig);
            this.partitionCount = originalConfig.getInt(Keys.PARTITION_COUNT, 1);
            this.topicName = SwiftUtils.extractTopicNameFromWriterConfig(writerConfig);

            if (StringUtils.isBlank(topicName)) {
                throw DataXException.asDataXException(WRITE_CONFIG_ERROR, "writer_config 中缺少 topicName");
            }

            swiftClient = new SwiftClient();
            try {
                swiftClient.init(clientConfig);
            } catch (SwiftException e) {
                throw DataXException.asDataXException(CLIENT_INIT_ERROR, CLIENT_INIT_ERROR.getDescription(), e);
            }


            //如果 topic 配置，则进行 topic 存在 检查,不存在则创建
            if (StringUtils.isNoneBlank(topicName)) {
                try {
                    SwiftUtils.checkTopicExists(swiftClient, topicName, partitionCount);
                } catch (SwiftException e) {
                    throw DataXException.asDataXException(TOPIC_CONFIG_ERROR, TOPIC_CONFIG_ERROR.getDescription(), e);
                }
            }


            LOG.info("swiftWriter job init completely");

        }


        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> list = Lists.newArrayList();
            for (int i = 0; i < mandatoryNumber; i++) {
                list.add(originalConfig.clone());
            }
            return list;
        }


        @Override
        public void destroy() {
            if (this.swiftClient != null) {
                this.swiftClient.close();
            }
        }
    }


    public static class Task extends Writer.Task {

        private final Logger LOG = LoggerFactory.getLogger(this.getClass());
        private Configuration sliceConfig;

        //swift client config
        private String clientConfig;

        //swift writer config
        private String writeConfig;


        private SwiftClient swiftClient;


        private com.alibaba.search.swift.SwiftWriter innerWriter;


        private List<Integer> hashFields = new ArrayList<Integer>();

        //filed->index 数据源到字段映射
        private List<String> indexNames = new ArrayList<String>();
        private long docLiveSeconds;

        @Override
        public void init() {
            this.sliceConfig = super.getPluginJobConf();
            this.clientConfig = parseClientConfig(this.sliceConfig);
            this.writeConfig = parseWriterConfig(this.sliceConfig);
            this.hashFields = parseHashField(this.sliceConfig);
            this.indexNames = parseIndexNames(this.sliceConfig);
            this.docLiveSeconds = parseDocLiveSeconds(this.sliceConfig);
            swiftClient = new SwiftClient();

            try {
                swiftClient.init(clientConfig);
            } catch (SwiftException e) {
                throw DataXException.asDataXException(CLIENT_INIT_ERROR, CLIENT_INIT_ERROR.getDescription(), e);
            }

            try {
                this.innerWriter = swiftClient.createWriter(writeConfig);
            } catch (SwiftException e) {
                throw DataXException.asDataXException(WRITER_CREATE_ERROR, WRITER_CREATE_ERROR.getDescription(), e);
            }


            LOG.info("swiftWriter task init completely....");

        }


        @Override
        public void startWrite(RecordReceiver lineReceiver) {

            int ok = 0;
            int error = 0;
            Record record = null;
            while (true) {
                record = record != null ? record : lineReceiver.getFromReader();
                if (record == null) {
                    break;
                }

                SwiftMessage.WriteMessageInfo.Builder builder = SwiftMessage.WriteMessageInfo.newBuilder();
                String hashStr = SwiftUtils.parseHashStr(record, hashFields);
                if (StringUtils.isNoneBlank(hashStr)) {
                    builder.setHashStr(ByteString.copyFrom(hashStr.getBytes()));
                }

                builder.setData(ByteString.copyFrom(SwiftUtils.record2Doc(record, indexNames, docLiveSeconds).getBytes()));

                try {
                    innerWriter.write(builder.build());
                    record = null;
                    ok++;
                } catch (SwiftException e) {
                    if (e.getEc() != ErrCode.ErrorCode.ERROR_CLIENT_SEND_BUFFER_FULL) {  //buffer满地异常需要重试
                        super.getTaskPluginCollector().collectDirtyRecord(record, e);
                        error++;
                        record = null;
                        LOG.error("write record error", e);
                    } else {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException ee) {
                        }
                    }
                }

            }

            try {
                innerWriter.waitFinished();
            } catch (SwiftException e) {
                throw DataXException.asDataXException(WRITER_WAIT_FINISHED, e);
            }

            LOG.info(this.getName() + " write record completely|success=" + ok + ",error=" + error);

        }


        @Override
        public void destroy() {
            if (this.innerWriter != null) {
                this.innerWriter.close();
            }

            if (this.swiftClient != null) {
                this.swiftClient.close();
            }
        }

    }


    public static final List<Integer> parseHashField(Configuration configuration) {
        List<Object> list = configuration.getList(Keys.HASH_FIELDS);
        List<Integer> hashFields = new ArrayList<Integer>();
        if (list != null && list.size() > 0) {
            for (Object it : list) {
                hashFields.add(Integer.valueOf(it.toString()));
            }
        }

        return hashFields;

    }


    public static final String parseClientConfig(Configuration configuration) {
        return configuration.getNecessaryValue(Keys.CLIENT_CONFIG, SwiftWriterErrorCode.CLIENT_CONFIG_ERROR);
    }

    public static final String parseWriterConfig(Configuration configuration) {
        return configuration.getNecessaryValue(Keys.WRITER_CONFIG, SwiftWriterErrorCode.WRITE_CONFIG_ERROR);

    }


    public static final List<String> parseIndexNames(Configuration configuration) {
        List<String> indexNames = new ArrayList<String>();
        List<Object> indexList = configuration.getList(Keys.INDEX_NAMES);
        if (indexList == null || indexList.size() == 0) {
            throw DataXException.asDataXException(INDEX_CONFIG_ERROR, INDEX_CONFIG_ERROR.getDescription());
        }

        if (indexList != null && indexList.size() > 0) {
            for (Object index : indexList) {
                indexNames.add(index.toString());
            }
        }

        return indexNames;
    }



    public static final Long parseDocLiveSeconds(Configuration configuration) {
        return configuration.getLong(Keys.DOC_LIVE_SECONDS, 0);
    }

}
