package com.alibaba.datax.plugin.reader.pgreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.test.simulator.BasicReaderPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

@RunWith(LoggedRunner.class)
public class PgReaderTest extends BasicReaderPluginTest {

	@TestLogger(log = "测试basic0.json. 配置一个jdbcUrl,一个table,运行时，通过程序自动生成 querySql 进行数据读取.")
	@Test
	public void testBasic0() {
		List<Record> noteRecordForTest = new ArrayList<Record>();

		List<Configuration> subjobs = super.doReaderTest("basic0.json", 0, noteRecordForTest);

		Assert.assertEquals(1, subjobs.size());
		Assert.assertTrue("记录总数应该是：每张表1条记录，一共1张表，共计1条记录.", noteRecordForTest.size() == 1);
	}
	
	@TestLogger(log = "测试basic1.json. 配置常量.")
	@Test
	public void testBasic1() {
		List<Record> noteRecordForTest = new ArrayList<Record>();

		List<Configuration> subjobs = super.doReaderTest("basic1.json", 0, noteRecordForTest);

		Assert.assertEquals(1, subjobs.size());
		Assert.assertTrue("记录总数应该是：每张表1条记录，一共1张表，共计1条记录.",
				noteRecordForTest.size() == 1);
	}

	@Override
	protected OutputStream buildDataOutput(String optionalOutputName) {
		File f = new File(optionalOutputName + "-output.txt");
		try {
			return new FileOutputStream(f);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String getTestPluginName() {
		return "pgreader";
	}

}
