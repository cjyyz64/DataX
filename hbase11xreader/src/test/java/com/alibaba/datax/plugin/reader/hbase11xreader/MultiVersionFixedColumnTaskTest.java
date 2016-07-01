package com.alibaba.datax.plugin.reader.hbase11xreader;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by shf on 16/3/31.
 */
public class MultiVersionFixedColumnTaskTest {

    @Test
    public void testFetchLine() throws Exception {
        RecordSender recordSender = mock(RecordSender.class);
        when(recordSender.createRecord()).thenReturn( new DefaultRecord());
        Record record = recordSender.createRecord();
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","users");
        String column = "[{\"name\":\"rowkey\",\"type\":\"string\"}," +
                "{\"name\":\"info:age\",\"type\":\"string\"}," +
                "{\"name\":\"info:birthday\",\"type\":\"date\",\"format\":\"yy-MM-dd\"}]";
        List columnjson = JSON.parseObject(column, new TypeReference<List>() {});
        configuration.set(Key.COLUMN,columnjson);
        configuration.set(Key.MODE,"multiVersionFixedColumn");
        configuration.set(Key.MAX_VERSION,-1);

        MultiVersionFixedColumnTask multiVersionFixedColumnTask = new MultiVersionFixedColumnTask(configuration);
        multiVersionFixedColumnTask.prepare();
        multiVersionFixedColumnTask.fetchLine(record);

        DateFormat dateFormat = new SimpleDateFormat("yy-MM-dd");
        Date datadate = dateFormat.parse("1987-06-17");
        Record record2 = new DefaultRecord();
        record2.addColumn(new StringColumn("lisi"));
        record2.addColumn(new StringColumn("info:age"));
        record2.addColumn(new LongColumn(1459068461919l));
        record2.addColumn(new StringColumn("30"));

        for(int i =0;i<record.getColumnNumber();i++){
            System.out.println(JSON.toJSONString(record.getColumn(i)));
            System.out.println(JSON.toJSONString(record2.getColumn(i)));
            assertEquals(JSON.toJSONString(record.getColumn(i)),JSON.toJSONString(record2.getColumn(i)));
        }
    }

}