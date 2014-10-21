package com.alibaba.datax.plugin.writer.mysqlwriter;

import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
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
public class MysqlWriterTest extends BasicWriterPluginTest {

	@TestLogger(log = "测试basic0.json. 配置一个jdbcUrl,一个table,运行时，通过程序自动生成 queryS1ql 进行数据读取.")
	@Test
	public void testBasic0() {
		int readerSliceNumber = 1;
		super.doWriterTest("basic0.json", readerSliceNumber);
	}

	@Override
	protected List<Record> buildDataForWriter() {
		List<Record> list = new ArrayList<Record>();
		Record r = new DefaultRecord();
		long dbId = 1L;
		int dbType = 3;
		String dbIp = "1.2.3.45";
		String dbPort = "8798";
		String dbRole = "slave";
		String dbName = "hello";
		String user = "root";
		String pass = "root";
		Date modifyTime = new Date();
		String modifyUser = "hejianchao";
		String description = "只是个描述哈";
		String tddlInfo = "{\"aa:bb\"}";

		r.addColumn(new LongColumn(dbId));
		r.addColumn(new LongColumn(dbType));
		r.addColumn(new StringColumn(dbIp));
		r.addColumn(new StringColumn(dbPort));
		r.addColumn(new StringColumn(dbRole));
		r.addColumn(new StringColumn(dbName));
		r.addColumn(new StringColumn(user));
		r.addColumn(new StringColumn(pass));
		r.addColumn(new DateColumn(modifyTime));

		r.addColumn(new StringColumn(modifyUser));
		r.addColumn(new StringColumn(description));
		r.addColumn(new StringColumn(tddlInfo));

		list.add(r);
		return list;
	}

	@Override
	public String getTestPluginName() {
		return "mysqlwriter";
	}

}
