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

        dynamicColumnConf.hbaseColumn=new DynamicColumnConf.HbaseColumn();
        dynamicColumnConf.hbaseColumn.rules= Lists.newArrayList();
        dynamicColumnConf.hbaseColumn.rules.add(hBaseRule1);

        dynamicColumnConf.hbaseTable="table123";
        dynamicColumnConf.rowkeyType="string";

        dynamicColumnConf.hbaseOutput="hbaseout123";
        dynamicColumnConf.hbaseConfig="hbasexml";
        dynamicColumnConf.hdfsConfig="hdfsxml";
        dynamicColumnConf.nullMode="EMPTY_BYTES";
        dynamicColumnConf.timeCol="2";
        dynamicColumnConf.encoding="utf-8";

        System.out.println(JSON.toJSONString(dynamicColumnConf));

        String result ="{\"encoding\":\"utf-8\",\"hbaseColumn\":{\"rules\":[{\"htype\":\"string\",\"pattern\":\"cf:col\"}],\"type\":\"prefix\"},\"hbaseConfig\":\"hbasexml\",\"hbaseOutput\":\"hbaseout123\",\"hbaseTable\":\"table123\",\"hdfsConfig\":\"hdfsxml\",\"nullMode\":\"EMPTY_BYTES\",\"rowkeyType\":\"string\",\"timeCol\":\"2\"}";

        Assert.assertEquals(result, JSON.toJSONString(dynamicColumnConf));
    }
}
