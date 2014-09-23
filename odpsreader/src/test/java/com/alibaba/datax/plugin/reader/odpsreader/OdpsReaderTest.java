package com.alibaba.datax.plugin.reader.odpsreader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.test.simulator.BasicReaderPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;

@RunWith(LoggedRunner.class)
public class OdpsReaderTest extends BasicReaderPluginTest {

	// TODO 添加自动化数据校验
	@TestLogger(log = "测试basic0.json. 读取分区表sale_detail的四个分区的数据，并配置了字段常量.")
	@Test
	public void testBasic0() {
		List<Record> noteRecordForTest = new ArrayList<Record>();

		super.doReaderTest("basic0.json", 1, noteRecordForTest);

		for (Record record : noteRecordForTest) {
			System.out.println(record);
		}
	}

	// TODO 添加自动化数据校验
	@TestLogger(log = "测试basic1.json. 读取分区表sale_detail的四个分区的数据，字段顺序调整，并且只读取部分列；分区配置为*.")
	@Test
	public void testBasic1() {
		List<Record> noteRecordForTest = new ArrayList<Record>();

		super.doReaderTest("basic1.json", 1, noteRecordForTest);

		for (Record record : noteRecordForTest) {
			System.out.println(record);
		}
	}

	// TODO 添加自动化数据校验
	@TestLogger(log = "测试basic2.json. 读取非分区表student_no_partition，列配置为*.")
	@Test
	public void testBasic2() {
		List<Record> noteRecordForTest = new ArrayList<Record>();

		super.doReaderTest("basic2.json", 1, noteRecordForTest);

		for (Record record : noteRecordForTest) {
			System.out.println(record);
		}
	}

	@Override
	protected OutputStream buildDataOutput(String optionalOutputName) {
//		File f = new File(optionalOutputName + "-output.txt");
//		try {
//			return new FileOutputStream(f);
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
		return null;
	}

	@Override
	protected String getTestPluginName() {
		return "odpsreader";
	}

}
