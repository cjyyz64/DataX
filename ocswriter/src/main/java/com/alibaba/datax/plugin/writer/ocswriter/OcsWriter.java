package com.alibaba.datax.plugin.writer.ocswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.ocswriter.utils.ConfigurationChecker;
import com.alibaba.datax.plugin.writer.ocswriter.utils.OcsWriterErrorCode;
import com.google.common.annotations.VisibleForTesting;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import net.spy.memcached.internal.OperationFuture;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

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
            //获取整个writer job的配置信息
            this.configuration = super.getPluginJobConf();
            //参数有效性检查
            ConfigurationChecker.check(this.configuration);
        }

        @Override
        public void prepare() {
            super.prepare();
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
        }
    }

    public static class Task extends Writer.Task {

        private Configuration configuration;
        private MemcachedClient client;
        private Set<Integer> indexesFromUser = new HashSet<Integer>();
        private String delimiter = "\u0001";
        private int expireTime;
        //private int batchSize;
        private ConfigurationChecker.WRITE_MODE writeMode;
        private TaskPluginCollector taskPluginCollector;

        @Override
        public void init() {
            this.configuration = this.getPluginJobConf();
            this.taskPluginCollector = super.getTaskPluginCollector();
        }

        @Override
        public void prepare() {
            super.prepare();

            String indexStr = this.configuration.getString(Key.INDEXES, "0");
            for (String index : indexStr.split(",")) {
                indexesFromUser.add(Integer.parseInt(index));
            }

            delimiter = this.configuration.getString(Key.FIELD_DELIMITER, "\u0001");
            expireTime = this.configuration.getInt(Key.EXPIRE_TIME, Integer.MAX_VALUE);
            //batchSize = this.configuration.getInt(Key.BATCH_SIZE, 1);
            writeMode = ConfigurationChecker.WRITE_MODE.valueOf(this.configuration.getString(Key.WRITE_MODE));

            String proxy = this.configuration.getString(Key.PROXY);
            String port = this.configuration.getString(Key.PORT, "11211");
            String username = this.configuration.getString(Key.USER);
            String password = this.configuration.getString(Key.PASSWORD);
            AuthDescriptor ad = new AuthDescriptor(new String[]{"PLAIN"}, new PlainCallbackHandler(username, password));

            try {
                client = getMemcachedConn(proxy, port, ad);
            } catch (Exception e) {
                throw DataXException.asDataXException(OcsWriterErrorCode.OCS_INIT_ERROR, String.format("初始化ocs客户端失败"));
            }
        }

        /**
         * 建立ocs客户端连接
         * 间隔1秒，重试10次失败
         */
        private MemcachedClient getMemcachedConn(final String proxy, final String port, final AuthDescriptor ad) {
            try {
                return RetryUtil.executeWithRetry(new Callable<MemcachedClient>() {
                    MemcachedClient memcachedConn;

                    @Override
                    public MemcachedClient call() throws Exception {
                        memcachedConn = new MemcachedClient(
                                new ConnectionFactoryBuilder().setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                                        .setAuthDescriptor(ad)
                                        .build(),
                                AddrUtil.getAddresses(proxy + ":" + port));
                        return memcachedConn;
                    }
                }, 10, 1000L, false);
            } catch (Exception e) {
                throw DataXException.asDataXException(OcsWriterErrorCode.OCS_INIT_ERROR, String.format("初始化ocs客户端失败"));
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            Record record;
            String key;
            String value;
            while ((record = lineReceiver.getFromReader()) != null) {
                try {
                    key = buildKey(record);
                    value = buildValue(record);
                    commit(key, value);
                } catch (DataXException e) {
                    this.taskPluginCollector.collectDirtyRecord(record, e);
                }
            }
        }

        private boolean commit(final String key, final String value) {
            try {
                return RetryUtil.executeWithRetry(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        OperationFuture<Boolean> future = null;
                        switch (writeMode) {
                            case set:
                                future = client.set(key, expireTime, value);
                                break;
                            case add:
                                future = client.add(key, expireTime, value);
                                break;
                            case replace:
                                future = client.replace(key, expireTime, value);
                                break;
                            case append:
                                future = client.append(0L, key, value);
                                break;
                            case prepend:
                                future = client.prepend(0L, key, value);
                                break;
                            default:
                                //因为前面参数校验的时候已经判断，不可能存在5中操作之外的类型。所以，不需要default。
                        }
                        if (future == null || future.getStatus() == null || !future.getStatus().isSuccess()) {
                            throw DataXException.asDataXException(OcsWriterErrorCode.COMMIT_FAILED, "提交数据失败");
                        }
                        return Boolean.TRUE;
                    }
                }, 10, 10L, true);
            } catch (Exception e) {
                throw DataXException.asDataXException(OcsWriterErrorCode.COMMIT_FAILED, "提交数据到ocs失败");
            }
        }

        /**
         * 构建key
         */

        private String buildValue(Record record) {
            ArrayList<String> valueList = new ArrayList<String>();
            int colNum = record.getColumnNumber();
            for (int i = 0; i < colNum; i++) {
                Column col = record.getColumn(i);
                if (col != null) {
                    String value;
                    Column.Type type = col.getType();
                    switch (type) {
                        case STRING:
                        case BOOL:
                        case DOUBLE:
                        case LONG:
                        case DATE:
                            value = col.asString();
                            break;
                        default:
                            //目前不支持二进制，如果遇到二进制，则当做脏数据处理
                            throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("不支持的数据格式:%s", type.toString()));
                    }
                    valueList.add(value);
                } else {
                    //如果取到的列为null,需要当做脏数据收集起来
                    throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("record中不存在第%s个字段", i));
                }
            }
            return StringUtils.join(valueList, delimiter);
        }

        /**
         * 构建value
         * 返回为空时当做脏数据处理
         */
        private String buildKey(Record record) {
            ArrayList<String> keyList = new ArrayList<String>();
            for (int index : indexesFromUser) {
                Column col = record.getColumn(index);
                if (col == null) {
                    throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("不存在第%s列", index));
                }
                Column.Type type = col.getType();
                switch (type) {
                    case STRING:
                    case BOOL:
                    case DOUBLE:
                    case LONG:
                    case DATE:
                        keyList.add(col.asString());
                        break;
                    default:
                        //目前不支持二进制，如果遇到二进制，则当做脏数据处理
                        throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("不支持的数据格式:%s", type.toString()));
                }
            }
            String rtn = StringUtils.join(keyList, delimiter);
            if (StringUtils.isBlank(rtn)) {
                throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("构建主键为空"));
            }
            return rtn;
        }

        @Override
        public void destroy() {
            if (client != null) {
                client.shutdown();
            }
        }

        /**
         * 以下为测试使用
         */
        @VisibleForTesting
        public String buildValue_test(Record record) {
            return this.buildValue(record);
        }

        @VisibleForTesting
        public String buildKey_test(Record record) {
            return this.buildKey(record);
        }

        @VisibleForTesting
        public void setIndexesFromUser(HashSet<Integer> indexesFromUser) {
            this.indexesFromUser = indexesFromUser;
        }

    }
}
