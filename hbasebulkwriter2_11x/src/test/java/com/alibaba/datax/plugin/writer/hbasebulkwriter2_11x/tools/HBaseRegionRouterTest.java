package com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x.tools;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;

import java.net.URL;

/**
 * no comments.
 * Created by liqiang on 16/4/4.
 */
public class HBaseRegionRouterTest {

    String hbaseConfig;
    @Before
    public void setUp() throws Exception {
        URL resource = Thread.currentThread().getContextClassLoader()
                .getResource("log4j.properties");
        System.out.println("resource = " + resource);
        PropertyConfigurator.configure(resource);
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.14:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"10.101.85.14\"}";

    }

    //需要设置hosts
//    @Test
//    public void testGetTabJson() throws Exception {
//        JSONArray rest= HBaseRegionRouter.getRegionsJson("bulkwriter211xtest_sort",null,hbaseConfig,null);
//        System.out.println(rest.toString());
//        Assert.assertTrue(rest.toString().contains("\"start\":\"\""));
//        //Assert.assertEquals(rest.toString(),"[{\"start\":\"\",\"end\":\"3130\"},{\"start\":\"3130\",\"end\":\"3230\"},{\"start\":\"3230\",\"end\":\"3330\"},{\"start\":\"3330\",\"end\":\"3430\"},{\"start\":\"3430\",\"end\":\"\"}]");
//    }
}