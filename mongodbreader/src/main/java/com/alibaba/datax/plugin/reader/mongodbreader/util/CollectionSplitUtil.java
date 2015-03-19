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

        List idList = doSplitRowId(adviceNumber,collection);
        for(Object obj : idList) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.SINCE_ID,obj);
            confList.add(conf);
        }
        return confList;
    }

    private static List doSplitRowId(int adviceNumber,DBCollection collection) {

        List idList = new ArrayList();

        long totalCount = collection.count();
        if(totalCount < 0) {
            return idList;
        }
        int batchSize = (int)totalCount/adviceNumber;

        DBObject sortObj = new BasicDBObject();
        sortObj.put("_id",1);
        for(int i = 1; i < adviceNumber; i++) {
            DBCursor cursor = collection.find().batchSize(1).sort(sortObj).skip(batchSize*i);
            if(!cursor.hasNext()) {
                break;
            }
            idList.add(cursor.next());
        }
        return idList;
    }
}
