package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.google.common.base.Strings;
import com.mongodb.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 */
public class CollectionSplitUtil {

    public static List<Configuration> doSplit(
            Configuration originalSliceConfig,int adviceNumber,MongoClient mongoClient) {

        List<Configuration> confList = new ArrayList<Configuration>();

        String dbName = originalSliceConfig.getString(KeyConstant.MONGO_DB_NAME);

        String collectionName = originalSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);

        if(Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(collectionName)) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
        }

        DB db = mongoClient.getDB(dbName);
        DBCollection collection = db.getCollection(collectionName);

        List<Entry> countInterval = doSplitInterval(adviceNumber,collection);
        for(Entry interval : countInterval) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.SKIP_COUNT,interval.interval);
            conf.set(KeyConstant.BATCH_SIZE,interval.batchSize);
            confList.add(conf);
        }
        return confList;
    }

    private static List<Entry> doSplitInterval(int adviceNumber,DBCollection collection) {

        List<Entry> intervalCountList = new ArrayList<Entry>();

        long totalCount = collection.count();
        if(totalCount < 0) {
            return intervalCountList;
        }
        long batchSize = totalCount/adviceNumber;
        for(int i = 0; i < adviceNumber; i++) {
            Entry entry = new Entry();
            entry.batchSize = batchSize;
            entry.interval = batchSize * i;
            intervalCountList.add(entry);
        }
        return intervalCountList;
    }

}

class Entry {
    Long interval;
    Long batchSize;
}
