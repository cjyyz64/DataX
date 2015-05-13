package com.alibaba.datax.core.writer.mysqlrulewriter.test;

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
 * Date: 15/5/10 下午7:09
 *
 * @author liupeng <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
@RunWith(LoggedRunner.class)
public class MysqlRuleWriterTest extends BasicWriterPluginTest {


    @TestLogger(log = "测试basic1.json. 配置多个jdbcUrl,多个table,运行时，通过程序自动生成 queryS1ql 进行数据读取.")
    @Test
    public void testBasic1() {
        int readerSliceNumber = 8;
        super.doWriterTest("basic1.json", readerSliceNumber);
    }

    @Override
    protected List<Record> buildDataForWriter() {
        List<Record> list = new ArrayList<Record>();
        for (int i = 0; i < 100; i++) {
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
        return list;
    }

    @Override
    protected String getTestPluginName() {
        return "mysqlrulewriter";
    }
}
