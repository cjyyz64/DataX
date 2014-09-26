package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OdpsWriterTest extends BasicWriterPluginTest {

	@Test
	public void testBasic0() {
		super.doWriterTest("basic0.json", 1);
	}

	@Override
	protected List<Record> buildDataForWriter() {
		List<Record> records = new ArrayList<Record>();
		Record r = new DefaultRecord();

		r.addColumn(new NumberColumn(1));
		r.addColumn(new StringColumn("hello-world"));
		r.addColumn(new DateColumn(new Date()));
		r.addColumn(new BoolColumn(false));
		r.addColumn(new NumberColumn(Math.PI));

		records.add(r);

		return records;
	}

	@Override
	protected String getTestPluginName() {
		return "odpswriter";
	}

}
