package com.alibaba.datax.plugin.writer.mongodbwriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.mongodbwriter.util.MongoUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/17 0017.
 */
public class MongoDBWriter extends Writer{

    public static class Job extends Writer.Job {

        private Configuration originalConfig = null;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configList = new ArrayList<Configuration>();
            for(int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.originalConfig.clone());
            }
            return configList;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {

        private static final Logger logger = LoggerFactory.getLogger(Task.class);
        private Configuration writerSliceConfig;
        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        private String database = null;
        private String collection = null;
        private Integer batchSize = null;
        private String splitter = " ";
        private JSONArray mongodbColumnMeta = null;
        private JSONObject upsertInfoMeta = null;
        private static int BATCH_SIZE = 1000;


        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            if(Strings.isNullOrEmpty(database) || Strings.isNullOrEmpty(collection)
                    || mongoClient == null || mongodbColumnMeta == null || batchSize == null) {
                throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE, "不合法参数");
            }
            DB db = mongoClient.getDB(database);
            DBCollection col = db.getCollection(this.collection);
            List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
            Record record = null;
            while((record = lineReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if(writerBuffer.size() >= this.batchSize) {
                    doBatchInsert(col,writerBuffer,mongodbColumnMeta);
                    writerBuffer.clear();
                }
            }
            if(!writerBuffer.isEmpty()) {
                doBatchInsert(col,writerBuffer,mongodbColumnMeta);
                writerBuffer.clear();
            }
        }

        private void doBatchInsert(DBCollection collection,List<Record> writerBuffer, JSONArray columnMeta) {

            List<DBObject> dataList = new ArrayList<DBObject>();

            for(Record record : writerBuffer) {

                BasicDBObject data = new BasicDBObject();

                for(int i = 0; i < record.getColumnNumber(); i++) {

                    if(Strings.isNullOrEmpty(record.getColumn(i).asString())) {

                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), record.getColumn(i).asString());
                        continue;
                    }
                    if(record.getColumn(i) instanceof StringColumn){
                        //处理数组类型
                        String type = columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_TYPE);
                        String splitter = columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_SPLITTER);
                        if(type.toLowerCase().equals("array")) {
                            System.out.println("type="+type+",splitter="+splitter);
                            if(splitter != null && splitter != "") {
                                throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE, "不合法参数");
                            }
                            data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), record.getColumn(i).asString().split(splitter));
                        } else {
                            data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), record.getColumn(i).asString());
                        }
                    } else if(record.getColumn(i) instanceof LongColumn) {

                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),record.getColumn(i).asLong());

                    } else if(record.getColumn(i) instanceof DateColumn) {

                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),record.getColumn(i).asDate());

                    } else if(record.getColumn(i) instanceof DoubleColumn) {

                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),record.getColumn(i).asDouble());

                    } else if(record.getColumn(i) instanceof BoolColumn) {

                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),record.getColumn(i).asBoolean());

                    } else if(record.getColumn(i) instanceof NullColumn) {

                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),null);

                    } else if(record.getColumn(i) instanceof BytesColumn) {

                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),record.getColumn(i).asBytes());

                    } else {
                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),record.getColumn(i).asString());
                    }
                }
                dataList.add(data);
            }
            /**
             * 如果存在重复的值覆盖
             */
            if(this.upsertInfoMeta != null) {
                BulkWriteOperation bulkUpsert = collection.initializeUnorderedBulkOperation();
                String uniqueKey = this.upsertInfoMeta.getString(KeyConstant.UNIQUE_KEY);
                if(!Strings.isNullOrEmpty(uniqueKey)) {
                    for(DBObject data : dataList) {
                        BasicDBObject query = new BasicDBObject();
                        if(uniqueKey != null) {
                            query.put(uniqueKey,data.get(uniqueKey));
                        }
                        bulkUpsert.find(query).upsert().replaceOne(data);
                    }
                    bulkUpsert.execute();
                } else {
                    throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE, "不合法参数");
                }
            } else {
                collection.insert(dataList);
            }
        }


        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.userName = writerSliceConfig.getString(KeyConstant.MONGO_USER_NAME);
            this.password = writerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
            if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(this.writerSliceConfig,userName,password);
            } else {
                this.mongoClient = MongoUtil.initMongoClient(this.writerSliceConfig);
            }
            this.database = writerSliceConfig.getString(KeyConstant.MONGO_DB_NAME);
            this.collection = writerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.batchSize = BATCH_SIZE;
            this.mongodbColumnMeta = JSON.parseArray(writerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.upsertInfoMeta = JSON.parseObject(writerSliceConfig.getString(KeyConstant.UPSERT_INFO));
        }

        @Override
        public void destroy() {
            mongoClient.close();
        }
    }

}
