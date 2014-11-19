package com.alibaba.datax.plugin.reader.oceanbasereader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.test.simulator.BasicReaderPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

@RunWith(LoggedRunner.class)
public class OceanbaseReaderTest extends BasicReaderPluginTest {

	@TestLogger(log = "测试basic0.json. 配置一个jdbcUrl,一个table,运行时，通过程序自动生成 querySql 进行数据读取.")
	@Test
	public void testBasic0() {
		List<Record> noteRecordForTest = new ArrayList<Record>();

		List<Configuration> subjobs = super.doReaderTest("basic0.json", 0,
				noteRecordForTest);

		Assert.assertEquals(1, subjobs.size());
		Assert.assertTrue("记录总数应该是：每张表4条记录，一共1张表，共计4条记录.",
				noteRecordForTest.size() == 47);
	}

	@TestLogger(log = "测试basic1.json. 配置一个jdbcUrl,一个querySql,运行时，直接执行 queryS1ql 进行数据读取.")
	@Test
	public void testBasic1() {
		List<Record> noteRecordForTest = new ArrayList<Record>();

		List<Configuration> subjobs = super.doReaderTest("basic1.json", 0,
				noteRecordForTest);

		Assert.assertEquals(1, subjobs.size());
		Assert.assertTrue("记录总数应该是：每张表4条记录，一共1张表，共计4条记录.",
				noteRecordForTest.size() == 4);
	}

	@TestLogger(log = "测试case1.json. 单库单表，但是配置多个ip进行尝试")
	@Test
	public void testCase1() {
		List<Record> noteRecordForTest = new ArrayList<Record>();

		List<Configuration> subjobs = super.doReaderTest("case1.json", 0,
				noteRecordForTest);

		Assert.assertEquals(1, subjobs.size());
		Assert.assertTrue("记录总数应该是：每张表4条记录，一共1张表，共计4条记录.",
				noteRecordForTest.size() == 4);
	}

	@TestLogger(log = "测试case2.json. 分库分表验证[1个库，共2条全表 querySql]. jdbcUrl + querySql ")
	@Test
	public void testCase2() {
		List<Record> noteRecordForTest = new ArrayList<Record>();

		List<Configuration> subjobs = super.doReaderTest("case2.json", 0,
				noteRecordForTest);

		Assert.assertEquals(2, subjobs.size());
		Assert.assertTrue("记录总数应该是：每张表4条记录，一共2条全表 querySql，共计8条记录.",
				noteRecordForTest.size() == 8);
	}

	@TestLogger(log = "测试case3.json，分库分表[1个库，共2张表]验证. jdbcUrl + table + where")
	@Test
	public void testCase3() {
		List<Record> noteRecordForTest = new ArrayList<Record>();

		List<Configuration> subjobs = super.doReaderTest("case3.json", 0,
				noteRecordForTest);
		Assert.assertEquals(2, subjobs.size());

		Assert.assertTrue(
				"记录总数应该是：每张表4条记录，一共2个分表，由于where过滤，每个表仅读取一条记录，共计2条记录.",
				noteRecordForTest.size() == 2);
	}

	@TestLogger(log = "测试case4.json，配置5个分表进行验证，但是框架建议切分为10份，用户未配置splitPk，所以切分份数最终只为表个数：5")
	@Test
	public void testCase4() {
		List<Record> noteRecordForTest = new ArrayList<Record>();
		int adviceSplitNumber = 10;
		List<Configuration> subjobs = super.doReaderTest("case4.json",
				adviceSplitNumber, noteRecordForTest);
		Assert.assertEquals(5, subjobs.size());

		Assert.assertTrue("记录总数应该是：每张表4条记录，一共5张表，共计20条记录.",
				noteRecordForTest.size() == 20);
	}

	@TestLogger(log = "测试case5.json，配置5个分表进行验证，但是框架建议切分为8份，用户配置splitPk，所以切分。但是切分总数是（8/5向上取整）*5 = 10")
	@Test
	public void testCase5() {
		List<Record> noteRecordForTest = new ArrayList<Record>();
		int adviceSplitNumber = 8;

		List<Configuration> subjobs = super.doReaderTest("case5.json",
				adviceSplitNumber, noteRecordForTest);
		Assert.assertEquals(10, subjobs.size());

		Assert.assertTrue("记录总数应该是：每张表4条记录，一共5张表，共计20条记录.",
				noteRecordForTest.size() == 20);
		for (Record r : noteRecordForTest) {
			System.out.println(r);
		}
	}

    @TestLogger(log = "测试case6_null.json，配置5个分表进行验证，但是框架建议切分为8份，用户配置splitPk，所以切分。但是切分总数是（8/5向上取整）*5 = 10")
    @Test
    public void testCase6_null() {
        List<Record> noteRecordForTest = new ArrayList<Record>();
        int adviceSplitNumber = 8;

        List<Configuration> subjobs = super.doReaderTest("case6_null.json",
                adviceSplitNumber, noteRecordForTest);
        for (Record r : noteRecordForTest) {
            System.out.println(r);
        }
    }


	@Override
	protected OutputStream buildDataOutput(String optionalOutputName) {
		return System.out;
	}

	@Override
	public String getTestPluginName() {
		return "oceanbasereader";
	}

}
