package com.alibaba.datax.plugin.writer.ocswriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.ocswriter.utils.CommonUtils;
import com.alibaba.datax.plugin.writer.ocswriter.utils.ConfigurationChecker;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Time:    2015-05-06 16:01
 * Creator: yuanqi@alibaba-inc.com
 */
public class OcsWriter extends Writer {

    static Logger logger = LoggerFactory.getLogger(OcsWriter.class);

    public static class Job extends Writer.Job {
        private Configuration configuration;

        @Override
        public void init() {
            /**
             * 获取整个writer job的配置信息
             */
            this.configuration = super.getPluginJobConf();

            /**
             * 参数有效性检查
             */
            ConfigurationChecker.check(this.configuration);
        }

        @Override
        public void prepare() {
            super.prepare();
            /**
             * Job开始之前无准备工作
             */
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            ArrayList<Configuration> configList = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.configuration.clone());
            }
            return configList;
        }

        @Override
        public void destroy() {
            /**
             * Job结束后无清理工作
             */
        }
    }

    public static class Task extends Writer.Task {

        private Configuration configuration;
        MemcachedClient client;
        Set<Integer> indexes = new HashSet<Integer>();
        String delimiter;
        int expireTime;
        int batchSize;

        @Override
        public void init() {
            this.configuration = this.getPluginJobConf();
        }

        @Override
        public void prepare() {
            super.prepare();

            String indexStr = this.configuration.getString(Key.INDEXES, "0");
            for (String index : indexStr.split(",")) {
                indexes.add(Integer.parseInt(index));
            }

            delimiter = this.configuration.getString(Key.FIELD_DELIMITER, "\u0001");
            expireTime = this.configuration.getInt(Key.EXPIRE_TIME, Integer.MAX_VALUE);
            batchSize = this.configuration.getInt(Key.BATCH_SIZE, 1);

            String proxy = this.configuration.getString(Key.PROXY);
            String port = this.configuration.getString(Key.PORT, "11211");
            String username = this.configuration.getString(Key.USER);
            String password = this.configuration.getString(Key.PASSWORD);

            AuthDescriptor ad = new AuthDescriptor(new String[]{"PLAIN"}, new PlainCallbackHandler(username, password));

            try {
                client = new MemcachedClient(
                        new ConnectionFactoryBuilder().setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                                .setAuthDescriptor(ad)
                                .build(),
                        AddrUtil.getAddresses(proxy + ":" + port));
            } catch (IOException e) {
                logger.error("", e);
            }
        }

        String key = null;
        String value = null;
        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            Record record = lineReceiver.getFromReader();
            key = buildKey(record);
            value = buildValue(record);
            while (!client.set(key, expireTime, value).getStatus().isSuccess()) {
                CommonUtils.sleepInMs(10L);
            }
        }

        ArrayList<String> tmpValue = new ArrayList<String>();
        /**
         * 构建key
         */
        private String buildValue(Record record) {
            tmpValue.clear();
            int colNum = record.getColumnNumber();
            for (int i = 0; i < colNum; i++) {
                tmpValue.add(record.getColumn(i).toString());
            }
            return StringUtils.join(tmpValue, delimiter);
        }

        ArrayList<String> tmpKey = new ArrayList<String>();
        /**
         * 构建value
         */
        private String buildKey(Record record) {
            tmpKey.clear();
            for (int index : indexes) {
                tmpKey.add(record.getColumn(index).asString());
            }
            return StringUtils.join(tmpKey, delimiter);
        }

        @Override
        public void destroy() {
            if (client != null) {
//                client.shutdown();
            }
        }
    }
}
