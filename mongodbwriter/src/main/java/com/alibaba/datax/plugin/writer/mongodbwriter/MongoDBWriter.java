package com.alibaba.datax.plugin.writer.mongodbwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.mongodbwriter.util.MongoUtil;
import com.google.common.base.Strings;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/17 0017.
 */
public class MongoDBWriter extends Writer{

    public static class Job extends Writer.Job {

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return null;
        }

        @Override
        public void init() {

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

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            if(Strings.isNullOrEmpty(database) || Strings.isNullOrEmpty(collection)) {
                return;
            }
            DB db = mongoClient.getDB(database);
            if(isAuth) {
                if(Strings.isNullOrEmpty(userName) || Strings.isNullOrEmpty(password)) {
                    return;
                }
                //TODO 补充一个验证方式
            }
            DBCollection collection = db.getCollection(this.collection);
            List<Record> witerBuffer = new ArrayList<Record>(this.batchSize);
            //TODO
        }

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.mongoClient = MongoUtil.initMongoClient(this.writerSliceConfig);
            this.isAuth = writerSliceConfig.getBool(KeyConstant.MONGO_IS_AUTH);
            if(this.isAuth) {
                this.userName = writerSliceConfig.getString(KeyConstant.MONGO_USER_NAME);
                this.password = writerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
            }
            this.database = writerSliceConfig.getString(KeyConstant.MONGO_DB_NAME);
            this.collection = writerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.batchSize = writerSliceConfig.getInt(KeyConstant.BATCH_SIZE);
        }

        @Override
        public void destroy() {

        }
    }

}
