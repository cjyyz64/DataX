package com.alibaba.datax.plugin.writer.hbasebulkwriter2.conf;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by liqiang on 15/5/25.
 */
public class FixColumnConfTest {

    @Test
    public void testNormal() throws Exception {
        FixColumnConf fixColumnConf=new FixColumnConf();

        FixColumnConf.HbaseColumn hbaseColumn1 = new FixColumnConf.HbaseColumn();
        hbaseColumn1.index="1";
        hbaseColumn1.hname="cf:name";
        hbaseColumn1.htype="string";

        FixColumnConf.HbaseColumn hbaseColumn2 = new FixColumnConf.HbaseColumn();
        hbaseColumn2.index="1";
        hbaseColumn2.hname="cf:age";
        hbaseColumn2.htype="int";
        fixColumnConf.hbaseColumn= Lists.newArrayList();
        fixColumnConf.hbaseColumn.add(hbaseColumn1);
        fixColumnConf.hbaseColumn.add(hbaseColumn2);

        FixColumnConf.RowkeyColumn rowkeyColumn1= new FixColumnConf.RowkeyColumn();
        rowkeyColumn1.index="0";
        rowkeyColumn1.htype="string";
        rowkeyColumn1.constant="xxx";

        fixColumnConf.hbaseRowkey=Lists.newArrayList();
        fixColumnConf.hbaseRowkey.add(rowkeyColumn1);

        fixColumnConf.hbaseTable="test123";
        fixColumnConf.hbaseOutput="output123";
        fixColumnConf.hbaseConfig="hbase.xml";
        fixColumnConf.hdfsConfig="hdfs.xml";
        fixColumnConf.nullMode="EMPTY_BYTES";
        fixColumnConf.timeCol="2";
        fixColumnConf.encoding="utf-8";

        System.out.println(JSON.toJSONString(fixColumnConf));

       String result="{\"encoding\":\"utf-8\",\"hbaseColumn\":[{\"hname\":\"cf:name\",\"htype\":\"string\",\"index\":\"1\"},{\"hname\":\"cf:age\",\"htype\":\"int\",\"index\":\"1\"}],\"hbaseConfig\":\"hbase.xml\",\"hbaseOutput\":\"output123\",\"hbaseRowkey\":[{\"constant\":\"xxx\",\"htype\":\"string\",\"index\":\"0\"}],\"hbaseTable\":\"test123\",\"hdfsConfig\":\"hdfs.xml\",\"nullMode\":\"EMPTY_BYTES\",\"timeCol\":\"2\"}";

        Assert.assertEquals(result, JSON.toJSONString(fixColumnConf));
    }

}
