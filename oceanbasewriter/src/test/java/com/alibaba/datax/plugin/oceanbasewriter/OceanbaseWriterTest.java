package com.alibaba.datax.plugin.oceanbasewriter;

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
public class OceanbaseWriterTest extends BasicWriterPluginTest {

	@TestLogger(log = "测试basic0.json")
	@Test
	public void testBasic0() {
		int readerSliceNumber = 1;
		super.doWriterTest("basic0.json", readerSliceNumber);
	}

	@Override
	protected List<Record> buildDataForWriter() {
		List<Record> list = new ArrayList<Record>();
		Record r = new DefaultRecord();
		r.addColumn(new LongColumn(6));
		r.addColumn(new StringColumn("api"));
        r.addColumn(new DoubleColumn("5.5"));
        r.addColumn(new DateColumn(new Date()));
        r.addColumn(new BoolColumn(true));
        list.add(r);
		return list;
	}

	@Override
	public String getTestPluginName() {
		return "oceanbasewriter";
	}

}
