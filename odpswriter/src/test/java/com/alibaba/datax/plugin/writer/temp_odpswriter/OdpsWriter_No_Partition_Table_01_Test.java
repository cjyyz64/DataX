package com.alibaba.datax.plugin.writer.temp_odpswriter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;

@RunWith(LoggedRunner.class)
public class OdpsWriter_No_Partition_Table_01_Test extends
		BasicWriterPluginTest {

	@TestLogger(log = "基本测试basic2，写入两行数据到一张非分区表。不能配置 partition，并且 column 配置为其中部分字段，顺序调整。写入份数为2份")
	@Test
	public void testBasic2() {
		super.doWriterTest("basic2.json", 2);
	}

	@Override
	protected List<Record> buildDataForWriter() {
		List<Record> records = new ArrayList<Record>();
		Record r1 = new DefaultRecord();

		r1.addColumn(new DateColumn(new Date()));
		r1.addColumn(new LongColumn(10009800));
		r1.addColumn(new DoubleColumn(Math.PI));

		Record r2 = new DefaultRecord();

		r2.addColumn(new DateColumn(new Date()));
		r2.addColumn(new LongColumn(122567));
		r2.addColumn(new DoubleColumn(Math.pow(Math.PI, Math.PI)));

		records.add(r1);
		records.add(r2);

		return records;
	}

	@Override
	protected String getTestPluginName() {
		return "odpswriter";
	}

}
