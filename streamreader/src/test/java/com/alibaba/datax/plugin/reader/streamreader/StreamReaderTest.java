package com.alibaba.datax.plugin.reader.streamreader;

import java.io.OutputStream;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.alibaba.datax.test.simulator.BasicReaderPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;

@RunWith(LoggedRunner.class)
public class StreamReaderTest extends BasicReaderPluginTest {

	@TestLogger(log = "测试basic.json")
	@Test
	public void testBasic() {
		super.doReaderTest("basic.json", 1, null);
	}

	@TestLogger(log = "测试case1.json")
	@Test
	public void test1() {
		super.doReaderTest("case1.json", 1, null);
	}

	@TestLogger(log = "测试case2.json")
	@Test
	public void test2() {
		super.doReaderTest("case2.json", 1, null);
	}

	@Override
	public String getTestPluginName() {
		return "streamreader";
	}

	@Override
	protected OutputStream buildDataOutput(String optionalOutputName) {
		return System.out;
	}

}
