package com.alibaba.datax.plugin.writer.zsearchwriter;

import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;
import com.alibaba.fastjson.JSONArray;
import junit.framework.Assert;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jxy on 5/24/16.
 */
public class BaiscTest extends BasicWriterPluginTest {
    Configuration configutation=getJobConf(TESTCLASSES_PATH + File.separator
            +"resources"+File.separator+"sample.json").getListConfiguration("job.content").get(0).getConfiguration("writer.parameter");
    @Override
    protected List<Record> buildDataForWriter() {
        List<Record> list=new ArrayList<Record>(10000);
        for(int i=0;i<10000;i++){
            Record record=new DefaultRecord();
            record.addColumn(new StringColumn("pk_"+i));
            record.addColumn(new StringColumn("String_"+i));
            record.addColumn(new DoubleColumn(i/10.0));
            record.addColumn(new LongColumn(i));
            record.addColumn(new StringColumn("HAHAHA"));
            list.add(record);
        }
        return list;
    }

    @Override
    protected String getTestPluginName() {
        return "zsearchwriter";
    }

    @Test
    public void testBufferBarrels() throws Exception {
        ZSearchConfig zsearchConfig= ZSearchConfig.of(configutation);
        BufferBarrels buffer=new BufferBarrels(zsearchConfig);
        List<Map<String,Object>> expectList=new ArrayList<Map<String, Object>>(10);
        for(int i=0;i<10;i++){
            Map<String,Object> map=new HashMap<String, Object>();
            map.put("id",i);
            map.put("context","t_"+i);
            expectList.add(map);
            buffer.addData(map);
        }
        Assert.assertEquals(JSONArray.toJSONString(expectList),buffer.getJSONData());
    }
    @Test
    public void testBufferBarrelsMaxSize() throws Exception {
        ZSearchConfig zsearchConfig= ZSearchConfig.of(configutation);
        BufferBarrels buffer=new BufferBarrels(zsearchConfig);
        String random=RandomStringUtils.randomAscii(1024*1024);
        for(int i=0;i<1000;i++){
            Map<String,Object> map=new HashMap<String, Object>();
            map.put("id",i);
            map.put("context",random);
            buffer.addData(map);
        }
        System.out.println(buffer.getJSONData());
    }
    @Test
    public void testStartWrite() throws Exception {
        ZSearchBatchWriter.Task task=new ZSearchBatchWriter.Task();
        task.setPluginJobConf(configutation);
        task.init();
        task.startWrite(super.createRecordReceiverForTest());
        Assert.assertEquals(0,task.getBarrels().getFailedCount());
    }

    @Test
    public void testJob() throws Exception {
        super.doWriterTest("it.json",2);
    }
}