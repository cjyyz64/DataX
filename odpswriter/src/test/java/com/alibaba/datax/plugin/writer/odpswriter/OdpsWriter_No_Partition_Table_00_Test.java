package com.alibaba.datax.plugin.writer.odpswriter;

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

@RunWith(LoggedRunner.class)
public class OdpsWriter_No_Partition_Table_00_Test extends BasicWriterPluginTest {

    @TestLogger(log = "基本测试basic0，写入两行数据到一张非分区表。不能配置 partition，并且 column 配置为*")
    @Test
    public void testBasic0() {
        super.doWriterTest("basic0.json", 1);
    }

    @TestLogger(log = "基本测试basic1，写入两行数据到一张非分区表。不能配置 partition，并且 column 配置为其中部分字段。写入份数为2份")
    @Test
    public void testBasic1() {
        super.doWriterTest("basic1.json", 2);
    }


    @Override
    protected List<Record> buildDataForWriter() {
        List<Record> records = new ArrayList<Record>();
        Record r1 = new DefaultRecord();

        r1.addColumn(new NumberColumn(1));
        r1.addColumn(new StringColumn("hello-world"));
        r1.addColumn(new DateColumn(new Date()));
        r1.addColumn(new BoolColumn(false));
        r1.addColumn(new NumberColumn(Math.PI));

        Record r2 = new DefaultRecord();

        r2.addColumn(new NumberColumn(1000000));
        r2.addColumn(new StringColumn("hello-阿里巴巴-DataX-world"));
        r2.addColumn(new DateColumn(new Date()));
        r2.addColumn(new BoolColumn(true));
        r2.addColumn(new NumberColumn(Math.PI * Math.PI));

        records.add(r1);
        records.add(r2);

        return records;
    }

    @Override
    protected String getTestPluginName() {
        return "odpswriter";
    }

}
