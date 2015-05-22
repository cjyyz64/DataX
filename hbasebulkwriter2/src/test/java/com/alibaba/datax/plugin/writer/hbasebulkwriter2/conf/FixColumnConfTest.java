package com.alibaba.datax.plugin.writer.hbasebulkwriter2.conf;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

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
        fixColumnConf.hbase_column= Lists.newArrayList();
        fixColumnConf.hbase_column.add(hbaseColumn1);
        fixColumnConf.hbase_column.add(hbaseColumn2);

        FixColumnConf.RowkeyColumn rowkeyColumn1= new FixColumnConf.RowkeyColumn();
        rowkeyColumn1.index="0";
        rowkeyColumn1.htype="string";
        rowkeyColumn1.constant="xxx";

        fixColumnConf.hbase_rowkey=Lists.newArrayList();
        fixColumnConf.hbase_rowkey.add(rowkeyColumn1);

        fixColumnConf.hbase_table="test123";
        fixColumnConf.hbase_output="output123";
        fixColumnConf.hbase_config="hbase.xml";
        fixColumnConf.hdfs_config="hdfs.xml";
        fixColumnConf.optional=new HashMap<String, String>();
        fixColumnConf.optional.put("null_mode","EMPTY_BYTES");

        System.out.println(JSON.toJSONString(fixColumnConf));

        String result="{\"hbase_column\":[{\"hname\":\"cf:name\",\"htype\":\"string\",\"index\":\"1\"},{\"hname\":\"cf:age\",\"htype\":\"int\",\"index\":\"1\"}],\"hbase_config\":\"hbase.xml\",\"hbase_output\":\"output123\",\"hbase_rowkey\":[{\"constant\":\"xxx\",\"htype\":\"string\",\"index\":\"0\"}],\"hbase_table\":\"test123\",\"hdfs_config\":\"hdfs.xml\",\"optional\":{\"null_mode\":\"EMPTY_BYTES\"}}";

        Assert.assertEquals(result,JSON.toJSONString(fixColumnConf));
    }

}
