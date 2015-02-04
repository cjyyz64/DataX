package com.alibaba.datax.plugin.writer.temp_odpswriter;

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
public class OdpsWriter_Partitioned_Table_00_Test extends BasicWriterPluginTest {

	@TestLogger(log = "基本测试basic3，写入两行数据到一张分区表。配置 partition，并且 column 配置为*")
	@Test
	public void testBasic3() {
		super.doWriterTest("basic3.json", 1);
	}

	@Override
	protected List<Record> buildDataForWriter() {
		List<Record> records = new ArrayList<Record>();
		Record r1 = new DefaultRecord();

		r1.addColumn(new LongColumn(1));
		r1.addColumn(new StringColumn("hello-world"));
		r1.addColumn(new DateColumn(new Date()));
		r1.addColumn(new BoolColumn(false));
		r1.addColumn(new DoubleColumn(Math.PI));

		Record r2 = new DefaultRecord();

		r2.addColumn(new LongColumn(1000000));
		r2.addColumn(new StringColumn("hello-阿里巴巴-DataX-world"));
		r2.addColumn(new DateColumn(new Date()));
		r2.addColumn(new BoolColumn(true));
		r2.addColumn(new DoubleColumn(Math.PI * Math.PI));

		records.add(r1);
		records.add(r2);

		return records;
	}

	@Override
	protected String getTestPluginName() {
		return "odpswriter";
	}

}
