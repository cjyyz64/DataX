package com.alibaba.datax.plugin.writer.oceanbasev10writer.test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;

/**
 * Date: 15/5/10 下午7:09
 *
 * @author liupeng <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
@RunWith(LoggedRunner.class)
public class OceanBaseV10WriterTest extends BasicWriterPluginTest {

    private boolean hasDirData = false;

    @TestLogger(log = "测试basic1.json. 配置多个jdbcUrl,多个table,运行时，通过程序自动生成 queryS1ql 进行数据读取.")
    @Test
    public void testBasic1() {
        hasDirData = false;
        int readerSliceNumber = 8;
        super.doWriterTest("basic1.json", readerSliceNumber);
    }

    @TestLogger(log = "测试basic2.json. 配置多个jdbcUrl,多库多表，表名都不同，分库名相同.")
    @Test
    public void testBasic2() {
        hasDirData = false;
        int readerSliceNumber = 8;
        super.doWriterTest("basic2.json", readerSliceNumber);
    }

    @TestLogger(log = "测试basic1.json. 配置多个jdbcUrl,多个table,运行时，有一条脏数据")
    @Test
    public void testDirDataBasic1() {
        hasDirData = true;
        int readerSliceNumber = 8;
        super.doWriterTest("basic1.json", readerSliceNumber);
        System.out.println(super.dirRecordList.size());
        Assert.assertEquals(dirRecordList.size(), 8);
    }

    @Override
    protected List<Record> buildDataForWriter() {
        List<Record> list = new ArrayList<Record>();
        for (int i = 0; i < 1000; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(i));
            r.addColumn(new LongColumn(6));
            r.addColumn(new LongColumn(6));
            r.addColumn(new StringColumn("api"));
            r.addColumn(new StringColumn("api"));
            r.addColumn(new DoubleColumn("5.5"));
            r.addColumn(new DoubleColumn("5.5"));
            r.addColumn(new BoolColumn(false));
            r.addColumn(new DateColumn(new Date()));
            list.add(r);
        }

        if(hasDirData) {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(101));
            r.addColumn(new StringColumn("abc"));
            r.addColumn(new StringColumn("abc"));
            r.addColumn(new StringColumn("api"));
            r.addColumn(new StringColumn("api"));
            r.addColumn(new DoubleColumn("5.5"));
            r.addColumn(new DoubleColumn("5.5"));
            r.addColumn(new BoolColumn(false));
            r.addColumn(new DateColumn(new Date()));
            list.add(r);
        }
        return list;
    }

    @Override
    protected String getTestPluginName() {
        return "mysqlrulewriter";
    }
}
