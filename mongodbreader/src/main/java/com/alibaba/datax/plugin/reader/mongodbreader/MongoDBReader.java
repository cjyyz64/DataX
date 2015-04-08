package com.alibaba.datax.plugin.reader.mongodbreader;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.MongoUtil;
import com.google.common.base.Strings;
import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 */
public class MongoDBReader extends Reader {

    private static final Logger logger = LoggerFactory.getLogger(MongoDBReader.class);

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig,adviceNumber,mongoClient);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
                this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME);
                this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
            if(!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig,userName,password);
            } else {
                this.mongoClient = MongoUtil.initMongoClient(originalConfig);
            }
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private static final Logger logger = LoggerFactory.getLogger(Task.class);
        private Configuration readerSliceConfig;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        private String database = null;
        private String collection = null;

        private String mongodbColumnMeta = null;
        private String skipCount = null;
        private Long batchSize = null;
        /**
         * 每页数据的大小
         */
        private int pageSize = 1000;

        @Override
        public void startRead(RecordSender recordSender) {

            if(Strings.isNullOrEmpty(skipCount) || batchSize == null ||
                             mongoClient == null || database == null ||
                             collection == null  || mongodbColumnMeta == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
            }
            List<String> columnMetaList = Arrays.asList(mongodbColumnMeta.split(","));
            DB db = mongoClient.getDB(database);
            DBCollection col = db.getCollection(this.collection);
            DBObject obj = new BasicDBObject();
            obj.put("_id",1);

            long pageCount = batchSize / pageSize;
            long modCount = batchSize % pageSize;

            for(int i = 0; i <= pageCount; i++) {
                skipCount += i * pageCount;
                if (i == pageCount) {
                    if (modCount == 0) {
                        break;
                    } else {
                        pageCount = modCount;
                    }
                }
                DBCursor dbCursor = col.find().sort(obj).skip(Integer.valueOf(skipCount)).limit((int) (long) pageCount);
                while (dbCursor.hasNext()) {
                    DBObject item = dbCursor.next();
                    Record record = recordSender.createRecord();
                    for (String column : columnMetaList) {
                        Object tempCol = item.get(column);
                        if (tempCol == null) {
                            continue;
                        }
                        if (tempCol instanceof Double) {
                            record.addColumn(new DoubleColumn((Double) tempCol));
                        } else if (tempCol instanceof Boolean) {
                            record.addColumn(new BoolColumn((Boolean) tempCol));
                        } else if (tempCol instanceof Date) {
                            record.addColumn(new DateColumn((Date) tempCol));
                        } else if (tempCol instanceof Integer || tempCol instanceof Long) {
                            record.addColumn(new LongColumn((Long) tempCol));
                        } else {
                            record.addColumn(new StringColumn(tempCol.toString()));
                        }
                    }
                    recordSender.sendToWriter(record);
                }
            }
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
                this.userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME);
                this.password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
            if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig,userName,password);
            } else {
                mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
            }
            this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME);
            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.mongodbColumnMeta = readerSliceConfig.getString(KeyConstant.MONGO_COLUMN);
            this.skipCount = readerSliceConfig.getString(KeyConstant.SKIP_COUNT);
            this.batchSize = readerSliceConfig.getLong(KeyConstant.BATCH_SIZE);
        }

        @Override
        public void destroy() {

        }
    }
}
