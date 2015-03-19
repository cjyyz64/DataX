package com.alibaba.datax.plugin.reader.mongodbreader;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.MongoUtil;
import com.mongodb.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 */
public class MongoDBReader extends Reader {

    private static final Logger logger = LoggerFactory.getLogger(MongoDBReader.class);

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private boolean isAuth = false;
        private String userName = null;
        private String password = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig,adviceNumber,mongoClient);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.isAuth = originalConfig.getBool(KeyConstant.MONGO_IS_AUTH);
            if(this.isAuth) {
                this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME);
                this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
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

        @Override
        public void startRead(RecordSender recordSender) {

        }

        @Override
        public void init() {

        }

        @Override
        public void destroy() {

        }
    }
}
