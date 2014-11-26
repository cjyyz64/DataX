package com.alibaba.datax.plugin.writer.pgwriter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;

@RunWith(LoggedRunner.class)
public class PgWriterTest extends BasicWriterPluginTest {

	@TestLogger(log = "测试basic0.json")
	@Test
	public void testBasic0() {
		super.doWriterTest("basic0.json", 1);
	}

	@Override
	protected List<Record> buildDataForWriter() {
		List<Record> list = new ArrayList<Record>();
		Record r = new DefaultRecord();
		String name = "xxx";
		Date signupDate = new Date();
		r.addColumn(new StringColumn(name));
		r.addColumn(new DateColumn(signupDate));
		list.add(r);
		return list;
	}

	@Override
	public String getTestPluginName() {
		return "pgwriter";
	}

}
