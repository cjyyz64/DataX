package com.alibaba.datax.plugin.writer.ocswriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Time:    2015-05-25 22:35
 * Creator: yuanqi@alibaba-inc.com
 */
@RunWith(LoggedRunner.class)
public class BasicOcsWriterTest extends BasicWriterPluginTest {
    @Override
    protected List<Record> buildDataForWriter() {
        List<Record> recordList = new ArrayList<Record>();
        for (int i = 0; i < 100; i++) {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("shit_"+i));
            record.addColumn(new BoolColumn(true));
            record.addColumn(new DoubleColumn(1113333333334.2222233333));
            record.addColumn(new LongColumn(123244444444444444L));
            record.addColumn(new DateColumn(new Date()));
        }
        return recordList;    }

    @Override
    protected String getTestPluginName() {
        return "ocswriter";
    }

    @TestLogger(log = "测试basic_0向ocs写入数据")
    @Test
    public void basicTest_0() {
        super.doWriterTest("basic_0.json", 1);
    }
}
