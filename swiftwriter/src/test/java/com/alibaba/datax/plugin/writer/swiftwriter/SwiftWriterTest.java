package com.alibaba.datax.plugin.writer.swiftwriter;

import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by zw86077 on 2015/12/9.
 */
public class SwiftWriterTest extends BasicWriterPluginTest {

    @TestLogger(log = "test")
    @Test
    public void testBasic0() {
        int readerSliceNumber = 1;
        super.doWriterTest("job.json", readerSliceNumber);
    }




    @Override
    protected List<Record> buildDataForWriter() {
        List<Record> list = new ArrayList<Record>();
        Record r = new DefaultRecord();
        String uuid = "11111111";
        String request_time = "2015-12-09 15:58;20";
        String host = "www.taobao.com";
        String uri = "/index.html";
        String method = "GET";
        r.addColumn(new StringColumn(uuid));
        r.addColumn(new StringColumn(request_time));
        r.addColumn(new StringColumn(host));
        r.addColumn(new StringColumn(uri));
        r.addColumn(new StringColumn(method));
        list.add(r);
        return list;
    }

    @Override
    protected String getTestPluginName() {
        return "swiftwriter";
    }
}
