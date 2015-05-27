import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.mongodbwriter.KeyConstant;
import com.alibaba.datax.plugin.writer.mongodbwriter.util.MongoUtil;
import com.mongodb.*;

/**
 * Created by jianying.wcj on 2015/3/22 0022.
 */
public class MongoAPITest {
    //db.tag_data2.insert({"sid":"123","name":"steven"})
    public static void main(String[] args) {

       /* Configuration conf = Configuration.newDefault();
        conf.set(KeyConstant.MONGO_HOST,"10.189.225.117");
        conf.set(KeyConstant.MONGO_PORT,27017);
        conf.set(KeyConstant.MONGO_IS_AUTH,"true");
        conf.set(KeyConstant.MONGO_USER_NAME,"trends");
        conf.set(KeyConstant.MONGO_USER_PASSWORD,"trends_2014");

        MongoClient mc = MongoUtil.initCredentialMongoClient(conf,"trends","trends_2014");
        String dbName = "tag_per_data";
        String collectionName = "tag_data1";
        DBCollection dbcol = mc.getDB(dbName).getCollection(collectionName);
        DBObject insertObj = mockObj();
        System.out.println(dbcol.findOne());*/
        //dbcol.insert(insertObj);
       /* DBObject query = new BasicDBObject("unique_id","7341944476_660018890_20026351228");
        BulkWriteOperation bwo = dbcol.initializeUnorderedBulkOperation();
        DBObject data = mockData();
        bwo.find(query).upsert().replaceOne(data);
        bwo.execute();*/
    }

    private static DBObject mockObj() {
        DBObject obj = new BasicDBObject();
        obj.put("unique_id","7341944476_660018890_20026351228");
        obj.put("sid","7341944476");
        obj.put("user_id","660018890");
        obj.put("auction_id","20026351228");
        obj.put("content_type","1");
        obj.put("pool_type","0");
        obj.put("frontcat_id","6");
        obj.put("categoryid","21 50010099 50023023");
        obj.put("gmt_create","1417506");
        obj.put("taglist","防雨布 加厚 防水布 雨布 油布 布 帆布 军工 元 雨蓬 防雨 防水 6.8 雨蓬布 幅 居家日用 伞 雨具 防雨 防潮 防雨布");
        obj.put("property","-1");
        obj.put("scorea","107859");
        obj.put("scoreb","994");
        return obj;
    }

    private static DBObject mockData() {
        DBObject obj = new BasicDBObject();
        obj.put("unique_id","7341944476_660018890_20026351228");
        obj.put("sid","123454");
        obj.put("user_id","660018890");
        obj.put("auction_id","20026351228");
        obj.put("content_type","1");
        obj.put("pool_type","0");
        obj.put("frontcat_id","6");
        obj.put("categoryid","21 50010099 50023023");
        obj.put("gmt_create","1417506");
        obj.put("taglist","防雨布 加厚 防水布 雨布 油布 布 帆布 军工 元 雨蓬 防雨 防水 6.8 雨蓬布 幅 居家日用 伞 雨具 防雨 防潮 防雨布");
        obj.put("property","-1");
        obj.put("scorea","107859");
        obj.put("scoreb","994");
        return obj;
    }
}
