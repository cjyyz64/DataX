package com.alibaba.datax.plugin.writer.zsearchwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.*;

/**
 * ZSearch批量写入
 */
public class ZSearchBatchWriter extends Writer {

    private static final Logger     log        = LoggerFactory.getLogger(ZSearchBatchWriter.class);
    private static       HttpClient httpClient = new DefaultHttpClient();

    /**
     * 清除表数据
     *
     * @param server
     * @param tableName
     */
    private static void doCleanup(String server, String tableName) {
        HttpDelete delete = new HttpDelete(String.format("%s/%s/_filter", server, tableName));
        delete.addHeader("admin", "nimda");
        httpCall(delete);
    }

    private static String httpCall(HttpRequestBase request) {
        try {
            HttpResponse resp = httpClient.execute(request);
            return EntityUtils.toString(resp.getEntity());
        } catch (IOException e) {
            throw DataXException
                    .asDataXException(ZSearchWriterErrorCode.BAD_CONFIG_VALUE, "zsearch服务端连接出错");
        }
    }

    /**
     * Job
     */
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration jobConfig     = null;
        private ZSearchConfig zSearchConfig = null;

        @Override
        public void init() {
            this.jobConfig = super.getPluginJobConf();
            this.zSearchConfig = ZSearchConfig.of(jobConfig);
            Vailidator.verify(zSearchConfig);
        }

        @Override
        public void prepare() {
            if (zSearchConfig.cleanup) {
                doCleanup(zSearchConfig.endpoint, zSearchConfig.accessId);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(jobConfig);
            }
            return configurations;
        }

        @Override
        public void post() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之后的后续处理，可以在此处完成。
             */
        }

        @Override
        public void destroy() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：通常配合 Job 中的 post() 方法一起完成 Job 的资源释放。
             */
        }

    }

    public static class Task extends Writer.Task {

        private Configuration taskConfig;
        private ZSearchConfig zSearchConfig;
        private BufferBarrels barrels;

        /* get barrels */
        public BufferBarrels getBarrels() {
            return barrels;
        }

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            this.zSearchConfig = ZSearchConfig.of(taskConfig);
            this.barrels = new BufferBarrels(zSearchConfig);
        }

        @Override
        public void prepare() {
            log.info("::::::::PREPARED::::::" + Thread.currentThread().getId());
        }


        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record = null;

            while ((record = recordReceiver.getFromReader()) != null) {
                // 组装数据
                Map<String, Object> data = new HashMap<String, Object>();
                for (int i = 0; i < record.getColumnNumber(); i++) {
                    Column column = record.getColumn(i);
                    Triple<String, String, Boolean> columnMeta = zSearchConfig.getColumnMeta(i);
                    String columnName = columnMeta.fst;
                    String columnType = columnMeta.snd;
                    // pk
                    if (columnName.equals(ZSearchConfig.PRIMARY_KEY_COLUMN_NAME)) {
                        String id = record.getColumn(i).asString();
                        try {
                            id = URLEncoder.encode(id, "UTF-8");
                            data.put("_id", id);
                        } catch (Exception e) {
                            super.getTaskPluginCollector().collectDirtyRecord(record, "主键不合法");
                        }
                        continue;
                    }
                    if (ZSearchConfig.TYPE_STRING.equals(columnType)) {
                        data.put(columnName, column.asString());
                    } else if (ZSearchConfig.TYPE_LONG.equals(columnType)) {
                        data.put(columnName, column.asLong());
                    } else if (ZSearchConfig.TYPE_DOUBLE.equals(columnType)) {
                        data.put(columnName, column.asDouble());
                    } else if (ZSearchConfig.TYPE_TEXT.equals(columnType)) {
                        columnName =
                                columnName.endsWith("_text") ? columnName : columnName + "_text";
                        data.put(columnName, column.asString());
                    } else {
                        //脏数据
                        getTaskPluginCollector().collectDirtyRecord(record,"类型错误:不支持的类型:" + columnType);
                    }
                }
                // 如果未定义主键添加UUID为PK
                if (!data.containsKey("_id")) {
                    data.put("_id", UUID.randomUUID().toString());
                }
                // 添加到队列 批次发送
                barrels.addData(data);
                barrels.tryFlush();
            }
            // 缓冲区余量
            barrels.forceFlush();

            log.info("失败导入总数:" + barrels.getFailedCount());
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {

        }

    }

    /**
     * Validate Config
     */
    private abstract static class Vailidator {

        public static void verify(ZSearchConfig conf) {
            notNull(conf.endpoint, "[$endpoint]zsearch endpoint 地址不能为空");
            notConnected(conf.endpoint, "[$endpoint]zsearch endpoint 无法连接");
            notNull(conf.accessId, "[$accessId]目标表名不能为空");
            notNull(conf.column, "[$column]映射的列配置不能为空");
            prepareMeta(conf.endpoint, conf.accessId, conf.accessKey, conf.columnMeta);
        }

        private static void notNull(String str, String message) {
            if ((str == null || "".equals(str))) {
                throw DataXException
                        .asDataXException(ZSearchWriterErrorCode.BAD_CONFIG_VALUE, message);
            }
        }

        private static void notNull(Collection coll, String message) {
            if ((coll == null || coll.size() == 0)) {
                throw DataXException
                        .asDataXException(ZSearchWriterErrorCode.BAD_CONFIG_VALUE, message);
            }
        }

        private static void notConnected(String server, String message) {
            HttpGet httpGet = new HttpGet(String.format("%s/ping", server));
            String ok = httpCall(httpGet);
            if (!"OK".equals(ok)) {
                throw DataXException
                        .asDataXException(ZSearchWriterErrorCode.BAD_CONFIG_VALUE, message);
            }
        }


        /**
         * 将data转成zsearch格式
         *
         * @param columnType
         * @param columnName
         * @param data
         */
        private static void putData(String columnType, String columnName, Map<String, Object> data) {
            if (ZSearchConfig.TYPE_STRING.equals(columnType)) {
                data.put(columnName, "sample_string");
            } else if (ZSearchConfig.TYPE_LONG.equals(columnType)) {
                data.put(columnName, 100000000000L);
            } else if (ZSearchConfig.TYPE_DOUBLE.equals(columnType)) {
                data.put(columnName, 1.23);
            } else if (ZSearchConfig.TYPE_TEXT.equals(columnType)) {
                columnName = columnName.endsWith("_text") ? columnName : columnName + "_text";
                data.put(columnName, "sample_text");
            } else {
                throw DataXException
                        .asDataXException(ZSearchWriterErrorCode.BAD_CONFIG_VALUE, "不支持的类型:" + columnType);
            }
        }

        /**
         * 发送假数据并删除
         *
         * @param server
         * @param tableName
         * @param token
         * @param data
         */
        private static void sendFakeData(String server, String tableName, String token, Map data) {
            try {
                //发送假数据
                HttpPost httpPost = new HttpPost(String
                        .format("%s/%s/datax_writer_add_attr?alive=%d", server, tableName, 10));
                httpPost.addHeader("token", token);
                httpPost.setEntity(new StringEntity(JSON.toJSONString(data)));
                String ok = httpCall(httpPost);
                if (!"OK".equals(ok)) {
                    throw DataXException
                            .asDataXException(ZSearchWriterErrorCode.BAD_CONFIG_VALUE, "密钥错误");
                }
                //删除假数据
                HttpDelete httpDelete = new HttpDelete(String
                        .format("%s/%s/datax_writer_add_attr", server, tableName));
                httpPost.addHeader("token", token);
                ok = httpCall(httpDelete);
                if (!"OK".equals(ok)) {
                    throw DataXException
                            .asDataXException(ZSearchWriterErrorCode.BAD_CONFIG_VALUE, "删除预设失败");
                }
            } catch (Exception e) {
                throw DataXException
                        .asDataXException(ZSearchWriterErrorCode.BAD_CONFIG_VALUE, "验证失败", e);
            }
        }

        /**
         * Meta准备,预先发送一个meta信息,强行正排和防止并发
         */
        private static void prepareMeta(String server, String tableName, String token, List<Triple<String, String, Boolean>> metaList) {
            Map<String, Object> data = new HashMap<String, Object>();
            Map<String, Object> other = new HashMap<String, Object>();
            //取出需要正排的字段
            for (Triple<String, String, Boolean> one : metaList) {
                String columnName = one.fst;
                String columnType = one.snd;
                //单条发送不需要pk
                if (columnName.equals(ZSearchConfig.PRIMARY_KEY_COLUMN_NAME)) {
                    continue;
                }
                if (one.trd != Boolean.TRUE) {
                    //不是正排
                    putData(columnType, columnName, other);
                } else {
                    //正排
                    //text字段不进正排
                    if (ZSearchConfig.TYPE_TEXT.equals(columnType)) {
                        continue;
                    }
                    putData(columnType, columnName, data);
                }
            }
            if (data.size() > 0) {
                //先保证正排
                sendFakeData(server, tableName, token, data);
            }
            if (other.size() > 0) {
                //再保证字段类型和防并发
                sendFakeData(server, tableName, token, other);
            }

        }

    }

}