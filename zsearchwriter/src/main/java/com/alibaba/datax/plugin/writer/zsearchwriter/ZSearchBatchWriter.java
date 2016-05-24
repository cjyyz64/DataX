package com.alibaba.datax.plugin.writer.zsearchwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
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

    private static final Logger log = LoggerFactory.getLogger(ZSearchBatchWriter.class);

    /**
     * Job
     */
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration jobConfig = null;
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
                doCleanup(zSearchConfig.server, zSearchConfig.tableName);
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
                    Pair<String, String> columnMeta = zSearchConfig.getColumnMeta(i);
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
                        columnName = columnName.endsWith("_text") ? columnName : columnName + "_text";
                        data.put(columnName, column.asString());
                    } else {
                        throw DataXException.asDataXException(ZSearchWriterErrorCode.BAD_CONFIG_VALUE, "不支持的类型:" + columnType);
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

            log.info("成功导入总数:" + barrels.getTotal());
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
            notNull(conf.server, "[$server]zsearch server 地址不能为空");
            notConnected(conf.server, "[$server]zsearch server 无法连接");
            notNull(conf.tableName, "[$tableName]目标表名不能为空");
            notNull(conf.column, "[$column]映射的列配置不能为空");
        }

        private static void notNull(String str, String message) {
            if ((str == null || "".equals(str))) {
                throw DataXException.asDataXException(ZSearchWriterErrorCode.BAD_CONFIG_VALUE, message);
            }
        }

        private static void notNull(Collection coll, String message) {
            if ((coll == null || coll.size() == 0)) {
                throw DataXException.asDataXException(ZSearchWriterErrorCode.BAD_CONFIG_VALUE, message);
            }
        }

        private static void notConnected(String server, String message) {
            HttpGet httpGet = new HttpGet(String.format("%s/ping", server));
            String ok = httpCall(httpGet);
            if (!"OK".equals(ok)) {
                throw DataXException.asDataXException(ZSearchWriterErrorCode.BAD_CONFIG_VALUE, message);
            }
        }

    }

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

    private static HttpClient httpClient = new DefaultHttpClient();

    private static String httpCall(HttpRequestBase request) {
        try {
            HttpResponse resp = httpClient.execute(request);
            return EntityUtils.toString(resp.getEntity());
        } catch (IOException e) {
            throw DataXException.asDataXException(ZSearchWriterErrorCode.BAD_CONFIG_VALUE, "zsearch服务端连接出错");
        }
    }


}