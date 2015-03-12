package com.alibaba.datax.plugin.reader.otsreader.perf;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.plugin.reader.otsreader.callable.GetRangeCallable;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.RetryHelper;
import com.aliyun.openservices.ots.OTSClientAsync;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRangeResult;
import com.aliyun.openservices.ots.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.model.RowPrimaryKey;

/**
 * 直接调用JAVA SDK
 * @author redchen
 *
 */

class MyThread extends Thread {
    private OTSClientAsync ots = new OTSClientAsync(
            Case1.endpoint,
            Case1.accessId,
            Case1.accesskey,
            Case1.instanceName);
    List<OTSRange> ranges = null;
    List<String> columnsToGet = null;
    
    public MyThread(List<OTSRange> ranges, List<String> columnsToGet) {
        this.ranges = ranges;
        this.columnsToGet = columnsToGet;
    }
    
    @Override
    public void run() {
        for (OTSRange range : ranges) {
            try {
                RowPrimaryKey token = range.getBegin();
                RangeRowQueryCriteria cur = new RangeRowQueryCriteria(Case1.tableName);
                cur.setDirection(Direction.FORWARD);
                cur.setColumnsToGet(columnsToGet);
                cur.setLimit(-1);
                cur.setExclusiveEndPrimaryKey(range.getEnd());
                
                do {
                    cur.setInclusiveStartPrimaryKey(token);
                    GetRangeRequest request = new GetRangeRequest();
                    request.setRangeRowQueryCriteria(cur);
                    GetRangeResult result = RetryHelper.executeWithRetry(
                            new GetRangeCallable(ots, cur, ots.getRange(request)),
                            12,
                            100
                            );
                    token = result.getNextStartPrimaryKey();
                    System.out.println(result.getRows().size());
                } while (token != null) ;
            } catch (Exception e) {
                e.printStackTrace();
            }
        
        }
        ots.shutdown();
    }
    
}

public class Case1 {
    
    public static String endpoint = "http://10.101.16.13";
    public static String accessId = "OTSMultiUser001_accessid";
    public static String accesskey = "OTSMultiUser001_accesskey";
    public static String instanceName = "TestInstance001";
    public static String tableName = "ots_datax_perf_new";

    public void run(int threadNum, String path) throws Exception {        
        List<String> columnsToGet = new ArrayList<String>();
        columnsToGet.add("userid");
        columnsToGet.add("groupid");
        
        columnsToGet.add("string_0");
        columnsToGet.add("string_1");
        columnsToGet.add("string_2");
        columnsToGet.add("string_3");
        columnsToGet.add("string_4");
        columnsToGet.add("string_5");
        columnsToGet.add("string_6");
        columnsToGet.add("string_7");
        columnsToGet.add("string_8");
        columnsToGet.add("string_9");
        columnsToGet.add("string_10");
        columnsToGet.add("string_11");
        columnsToGet.add("string_12");
        columnsToGet.add("string_13");
        columnsToGet.add("string_14");
        columnsToGet.add("int_0");
        columnsToGet.add("int_1");

        RangeParse p = new RangeParse(path);
        List<OTSRange> ranges = p.getRange();
        
        List<List<OTSRange>> rss = new ArrayList<List<OTSRange>>();
        
        for (int i = 0; i < threadNum; i++) {
            rss.add(new ArrayList<OTSRange>());
        }
        
        for (int i = 0; i < ranges.size(); i++) {
            int k = i % threadNum;
            rss.get(k).add(ranges.get(i));
        }
        
        for (List<OTSRange> cs : rss) {
            MyThread my = new MyThread(cs, columnsToGet);
            my.start();
        }
    }

    public static void main(String[] args) throws Exception {
        for (String s: args) {
            System.out.println(s);
        }
        int threadNum = 1;
        String path  = "src/test/resources/range.txt";
        if (args.length == 2) {
            threadNum = Integer.parseInt(args[0]);
            path = args[1];
        }
        Case1 sdk = new Case1();
        sdk.run(threadNum, path);
    }

}
