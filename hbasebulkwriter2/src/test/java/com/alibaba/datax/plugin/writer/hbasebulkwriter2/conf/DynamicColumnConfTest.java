package com.alibaba.datax.plugin.writer.hbasebulkwriter2.conf;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by liqiang on 15/5/25.
 */
public class DynamicColumnConfTest {
    @Test
    public void testNormal() throws Exception {
        DynamicColumnConf dynamicColumnConf=new DynamicColumnConf();

        DynamicColumnConf.HBaseRule hBaseRule1 = new DynamicColumnConf.HBaseRule();
        hBaseRule1.pattern="cf:col";
        hBaseRule1.htype="string";

        dynamicColumnConf.hbase_column=new DynamicColumnConf.HbaseColumn();
        dynamicColumnConf.hbase_column.rules= Lists.newArrayList();
        dynamicColumnConf.hbase_column.rules.add(hBaseRule1);

        dynamicColumnConf.hbase_table="table123";
        dynamicColumnConf.rowkey_type="string";

        dynamicColumnConf.hbase_output="hbaseout123";
        dynamicColumnConf.hbase_config="hbasexml";
        dynamicColumnConf.hdfs_config="hdfsxml";

        System.out.println(JSON.toJSONString(dynamicColumnConf));

        String result ="{\"hbase_column\":{\"rules\":[{\"htype\":\"string\",\"pattern\":\"cf:col\"}],\"type\":\"prefix\"},\"hbase_config\":\"hbasexml\",\"hbase_output\":\"hbaseout123\",\"hbase_table\":\"table123\",\"hdfs_config\":\"hdfsxml\",\"rowkey_type\":\"string\"}";

        Assert.assertEquals(result,JSON.toJSONString(dynamicColumnConf));
    }
}
