import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.mongodbwriter.KeyConstant;
import com.alibaba.datax.plugin.writer.mongodbwriter.util.MongoUtil;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;
import com.alibaba.fastjson.JSON;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/22 0022.
 */
@RunWith(LoggedRunner.class)
public class MongoAPITest extends BasicWriterPluginTest {
    //db.tag_data2.insert({"sid":"123","name":"steven"})
    @Test
    public void mongoApi() {
        Configuration conf = Configuration.newDefault();
        List adds = new ArrayList();
//        adds.add("100.69.195.221:33017");
        adds.add("100.69.198.134:33017");
//        adds.add("100.69.202.74:33017");
        conf.set(KeyConstant.MONGO_ADDRESS,adds);
        conf.set(KeyConstant.MONGO_USER_NAME,"relitu");
        conf.set(KeyConstant.MONGO_USER_PASSWORD,"relitu&123");
        MongoClient mc = MongoUtil.initCredentialMongoClient(conf,"relitu","relitu&123","relitu");
        String dbName = "relitu";
        String collectionName = "ws";
        DBCollection dbcol = mc.getDB(dbName).getCollection(collectionName);
        DBObject insertObj = mockObj();
        dbcol.insert(insertObj);
       /* DBObject query = new BasicDBObject("unique_id","7341944476_660018890_20026351228");
        BulkWriteOperation bwo = dbcol.initializeUnorderedBulkOperation();
        DBObject data = mockData();
        bwo.find(query).upsert().replaceOne(data);
        bwo.execute();*/
        System.out.println(dbcol.find());
    }

    @TestLogger(log = "测试basic0.json. 配置一个mongodbUrl,构造数据,运行时，写入mongodb.")
    @Test
    public void testBasic0() {
        int readerSliceNumber = 1;
        super.doWriterTest("basic0.json", readerSliceNumber);
    }

    private static DBObject mockObj() {
        DBObject obj = new BasicDBObject();
//        obj.put("unique_id","7341944476_660018890_20026351228");
        obj.put("sid",7341944476l);
        obj.put("user_id","660018890");
        obj.put("auction_id","20026351228");
        DataMode mode = new DataMode();
        mode.setType("point");
        Double[] dd = {123.45,986.3};
        mode.setCoord(dd);
        obj.put("content_type",mode);
//        obj.put("pool_type","0");
//        obj.put("frontcat_id","6");
//        obj.put("categoryid","21 50010099 50023023");
//        obj.put("gmt_create","1417506");
//        obj.put("taglist","防雨布 加厚 防水布 雨布 油布 布 帆布 军工 元 雨蓬 防雨 防水 6.8 雨蓬布 幅 居家日用 伞 雨具 防雨 防潮 防雨布");
//        obj.put("property","-1");
//        obj.put("scorea","107859");
//        obj.put("scoreb","994");
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

    @Test
    public void build(){
        Record r = new DefaultRecord();
        String pid = "xxx";
        double[]  loc = {116.401884,39.877865};
        String[] epsg3857 = {"12957798.454257699","4848209.794819011"};
        String geohid ="xxx";
        int    pv = 2000;
        String scenicId = "xxx";
        r.addColumn(new StringColumn(pid));
        r.addColumn(new StringColumn(Arrays.toString(loc)));
        r.addColumn(new StringColumn(Arrays.toString(epsg3857)));
        r.addColumn(new StringColumn(geohid));
        r.addColumn(new LongColumn(pv));
        r.addColumn(new StringColumn(scenicId));
        System.err.println(JSON.toJSON(loc));
        System.err.println(JSON.toJSON(epsg3857));
        System.err.println(Column.Type.DOUBLE.name());
        DataMode mode = new DataMode();
        mode.setType("point");
        Double[] dd = {123.45,986.3,520.0130};
        mode.setCoord(dd);
        System.err.println(JSON.toJSONString(mode));

    }

    @Override
    protected List<Record> buildDataForWriter() {
        List<Record> list = new ArrayList<Record>();
        Record r = new DefaultRecord();
        String pid = "xxx";
        String loc = "116,41,84,39,87,85";
        String epsg3857 = "12957798.454257699,4848209.794819011";
        String geohid ="wss";
        int    pv = 2000;
        String scenicId = "{\"type\":\"point\",\"coord\":[923.23,546.12]}";
        DataMode mode = new DataMode();
        mode.setType("point");
        Double[] dd = {13.45,26.3,112.1};
        mode.setCoord(dd);
        Long[] ll = {123123l,456456l,789789l};
        mode.setLongs(ll);
        Integer[] ii = {123,456,789};
        mode.setIntegers(ii);
        String[] ss = {"qwe","asd","zxc"};
        mode.setStrings(ss);
        r.addColumn(new StringColumn(pid));
        r.addColumn(new StringColumn(loc));
        r.addColumn(new StringColumn(epsg3857));
        r.addColumn(new DateColumn(new Date()));
        r.addColumn(new LongColumn(pv));
        r.addColumn(new StringColumn(JSON.toJSONString(mode)));
        list.add(r);
        return list;
    }

    @Override
    protected String getTestPluginName() {
        return "mongodbwriter";
    }
}
