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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Time:    2015-05-06 16:01
 * Creator: yuanqi@alibaba-inc.com
 * TODO 1，控制速度 2、MemcachedClient连接超时情况处理
 */
public class OcsWriter extends Writer {

    public static class Job extends Writer.Job {
        private Configuration configuration;

        @Override
        public void init() {
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
        private String delimiter;
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

            //如果用户不配置，默认为第0列
            String indexStr = this.configuration.getString(Key.INDEXES, "0");
            for (String index : indexStr.split(",")) {
                indexesFromUser.add(Integer.parseInt(index));
            }

            //如果用户不配置，默认为\u0001
            delimiter = this.configuration.getString(Key.FIELD_DELIMITER, "\u0001");
            expireTime = this.configuration.getInt(Key.EXPIRE_TIME, 0);
            //todo 此版本不支持批量提交，待ocswriter发布新版本client后支持。batchSize = this.configuration.getInt(Key.BATCH_SIZE, 100);
            writeMode = ConfigurationChecker.WRITE_MODE.valueOf(this.configuration.getString(Key.WRITE_MODE));

            String proxy = this.configuration.getString(Key.PROXY);
            //默认端口为11211
            String port = this.configuration.getString(Key.PORT, "11211");
            String username = this.configuration.getString(Key.USER);
            String password = this.configuration.getString(Key.PASSWORD);
            AuthDescriptor ad = new AuthDescriptor(new String[]{"PLAIN"}, new PlainCallbackHandler(username, password));

            try {
                client = getMemcachedConn(proxy, port, ad);
            } catch (Exception e) {
                //异常不能吃掉，直接抛出，便于定位
                throw DataXException.asDataXException(OcsWriterErrorCode.OCS_INIT_ERROR, String.format("初始化ocs客户端失败"), e);
            }
        }

        /**
         * 建立ocs客户端连接
         * 重试10次，间隔时间指数增长
         */
        private MemcachedClient getMemcachedConn(final String proxy, final String port, final AuthDescriptor ad) throws Exception {
            return RetryUtil.executeWithRetry(new Callable<MemcachedClient>() {
                @Override
                public MemcachedClient call() throws Exception {
                    return new MemcachedClient(
                            new ConnectionFactoryBuilder().setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                                    .setAuthDescriptor(ad)
                                    .build(),
                            AddrUtil.getAddresses(proxy + ":" + port));
                }
            }, 10, 1000L, true);
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
                } catch (Exception e) {
                    this.taskPluginCollector.collectDirtyRecord(record, e);
                }
            }
        }

        /**
         * 提交数据到ocs，有重试机制
         */
        private boolean commit(final String key, final String value) throws Exception {
            return RetryUtil.executeWithRetry(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    OperationFuture<Boolean> future = null;
                    switch (writeMode) {
                        case set:
                            future = client.set(key, expireTime, value);
                            break;
                        case add:
                            //幂等原则：相同的输入得到相同的输出，不管调用多少次。
                            //所以add和replace是幂等的。
                            future = client.add(key, expireTime, value);
                            break;
                        case replace:
                            future = client.replace(key, expireTime, value);
                            break;
                        //todo 【注意】append和prepend重跑任务不能支持幂等，使用需谨慎
                        case append:
                            future = client.append(0L, key, value);
                            break;
                        case prepend:
                            future = client.prepend(0L, key, value);
                            break;
                        default:
                            //因为前面参数校验的时候已经判断，不可能存在5中操作之外的类型。所以，不需要default。
                    }
                    //【注意】getStatus()返回为null有可能是因为get()超时导致，此种情况当做脏数据处理。但有可能数据已经成功写入ocs。
                    if (future == null || future.getStatus() == null || !future.getStatus().isSuccess()) {
                        throw DataXException.asDataXException(OcsWriterErrorCode.COMMIT_FAILED, "提交数据到ocs失败");
                    }
                    return Boolean.TRUE;
                }
            }, 3, 1000L, false);
        }

        /**
         * 构建value
         * 如果有二进制字段当做脏数据处理
         * 如果col为null，当做脏数据处理
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
                            //【注意】value字段中如果有分隔符，当做脏数据处理
                            if (value != null && value.contains(delimiter)) {
                                throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("数据中包含分隔符:%s", value));
                            }
                            break;
                        default:
                            //目前不支持二进制，如果遇到二进制，则当做脏数据处理
                            throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("不支持的数据格式:%s", type.toString()));
                    }
                    valueList.add(value);
                } else {
                    //如果取到的列为null,需要当做脏数据处理
                    throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("record中不存在第%s个字段", i));
                }
            }
            return StringUtils.join(valueList, delimiter);
        }

        /**
         * 构建key
         * 构建数据为空时当做脏数据处理
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
                throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("构建主键为空，请检查indexes的配置"));
            }
            return rtn;
        }

        /**
         * shutdown中会有数据异步提交，等待5min。
         * 1、此处有可能抛出RuntimeException，如果遇到RuntimeException则任务失败。
         * 2、不能保证5分钟后所有数据都已经提交到ocs，所以5min之内还未返回true，则认为任务失败。
         */
        @Override
        public void destroy() {
            try {
                if (client == null || client.shutdown(300L, TimeUnit.SECONDS)) {
                    return;
                }
                throw DataXException.asDataXException(OcsWriterErrorCode.SHUTDOWN_FAILED, "关闭ocsClient失败");
            } catch (Exception e) {
                throw DataXException.asDataXException(OcsWriterErrorCode.SHUTDOWN_FAILED, "关闭ocsClient时发生异常", e);
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
