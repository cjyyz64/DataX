package com.alibaba.datax.plugin.writer.mongodbwriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.mongodbwriter.util.MongoUtil;
import com.google.common.base.Strings;
import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

        private boolean isAuth = false;
        private String userName = null;
        private String password = null;
        private String database = null;
        private String collection = null;
        private Integer batchSize = null;
        private boolean isContainArray = false;
        private String splitter = " ";

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            if(Strings.isNullOrEmpty(database) || Strings.isNullOrEmpty(collection)) {
                return;
            }
            DB db = mongoClient.getDB(database);
            DBCollection col = db.getCollection(this.collection);
            List<String> columnMetaList = new ArrayList<String>();
            columnMetaList.addAll(col.findOne().keySet());
            List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
            Record record = null;
            while((record = lineReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if(writerBuffer.size() >= this.batchSize) {
                    doBatchInsert(col,writerBuffer,columnMetaList.subList(1,columnMetaList.size()));
                    writerBuffer.clear();
                }
            }
            if(!writerBuffer.isEmpty()) {
                doBatchInsert(col,writerBuffer,columnMetaList.subList(1,columnMetaList.size()));
                writerBuffer.clear();
            }
        }

        private void doBatchInsert(DBCollection collection,List<Record> writerBuffer, List<String> columnMetaList) {

            List<DBObject> dataList = new ArrayList<DBObject>();

            for(Record record : writerBuffer) {

                BasicDBObject data = new BasicDBObject();

                for(int i = 0; i < record.getColumnNumber(); i++) {

                    if(Strings.isNullOrEmpty(record.getColumn(i).asString())) {

                        data.put(columnMetaList.get(i), record.getColumn(i).asString());
                        continue;
                    }
                    if(record.getColumn(i) instanceof StringColumn){
                        //处理数组类型
                        if(this.isContainArray) {
                            if(!Strings.isNullOrEmpty(this.splitter)) {
                                logger.warn("columnMeta="+columnMetaList.get(i)+" record="+record+" record.getColumn("+i+")="+record.getColumn(i).asString());
                                data.put(columnMetaList.get(i), record.getColumn(i).asString().split(this.splitter));
                            }
                        } else {
                            data.put(columnMetaList.get(i), record.getColumn(i).asString());
                        }

                    } else if(record.getColumn(i) instanceof LongColumn) {

                        data.put(columnMetaList.get(i),record.getColumn(i).asLong());

                    } else if(record.getColumn(i) instanceof DateColumn) {

                        data.put(columnMetaList.get(i),record.getColumn(i).asDate());

                    } else if(record.getColumn(i) instanceof DoubleColumn) {

                        data.put(columnMetaList.get(i),record.getColumn(i).asDouble());

                    } else if(record.getColumn(i) instanceof BoolColumn) {

                        data.put(columnMetaList.get(i),record.getColumn(i).asBoolean());

                    } else if(record.getColumn(i) instanceof NullColumn) {

                        data.put(columnMetaList.get(i),null);

                    } else if(record.getColumn(i) instanceof BytesColumn) {

                        data.put(columnMetaList.get(i),record.getColumn(i).asBytes());

                    } else {

                        data.put(columnMetaList.get(i),record.getColumn(i).asString());
                    }
                }
                dataList.add(data);
                collection.insert(dataList);
            }
        }


        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();

            this.isAuth = writerSliceConfig.getBool(KeyConstant.MONGO_IS_AUTH);
            if(this.isAuth) {
                this.userName = writerSliceConfig.getString(KeyConstant.MONGO_USER_NAME);
                this.password = writerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
                if(isAuth) {
                    if(Strings.isNullOrEmpty(userName) || Strings.isNullOrEmpty(password)) {
                        return;
                    }
                    this.mongoClient = MongoUtil.initCredentialMongoClient(this.writerSliceConfig,userName,password);
                }
            } else {
                this.mongoClient = MongoUtil.initMongoClient(this.writerSliceConfig);
            }
            this.database = writerSliceConfig.getString(KeyConstant.MONGO_DB_NAME);
            this.collection = writerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.batchSize = writerSliceConfig.getInt(KeyConstant.BATCH_SIZE);
            this.isContainArray = writerSliceConfig.getBool(KeyConstant.IS_CONTAIN_ARRAY);
            this.splitter = writerSliceConfig.getString(KeyConstant.ARRAY_SPLITTER);
        }

        @Override
        public void destroy() {
            mongoClient.close();
        }
    }

}
