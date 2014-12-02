package com.alibaba.datax.plugin.writer.pgwriter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;

@RunWith(LoggedRunner.class)
public class PgWriterTest extends BasicWriterPluginTest {
	
	private int whichCase = 0;

	@TestLogger(log = "测试basic0.json")
	@Test
	public void testBasic0() {
		whichCase = 0;
		super.doWriterTest("basic0.json", 1);
	}
	
	@TestLogger(log = "测试basic1.json")
	@Test
	public void testBasic1() {
		whichCase = 1;
		super.doWriterTest("basic1.json", 1);
	}

	@Override
	protected List<Record> buildDataForWriter() {
		if(whichCase==0){
			return buildDataForCase0();
		}
		if(whichCase==1){
			return buildDataForCase1();
		}
		return null;
	}
	
	private List<Record> buildDataForCase0(){
		List<Record> list = new ArrayList<Record>();
		Record r = new DefaultRecord();
		r.addColumn(new LongColumn(-1L));
		r.addColumn(new StringColumn("xxx"));
		r.addColumn(new DateColumn(new Date()));
		list.add(r);
		return list;
	}
	
	private List<Record> buildDataForCase1(){
		List<Record> list = new ArrayList<Record>();
		Record r = new DefaultRecord();
		r.addColumn(new LongColumn(99999));
		r.addColumn(new LongColumn(12345));
		r.addColumn(new StringColumn("1010000000"));
		r.addColumn(new StringColumn("1"));
		r.addColumn(new BytesColumn());
		r.addColumn(new StringColumn("aa"));
		r.addColumn(new StringColumn("2011-11-11 11:11:11"));
		r.addColumn(new DoubleColumn("2.2"));
		r.addColumn(new LongColumn("1"));
		r.addColumn(new DoubleColumn("100.3"));
		r.addColumn(new DoubleColumn("4.4"));
		r.addColumn(new DoubleColumn("5.6"));
		r.addColumn(new LongColumn("1"));
		r.addColumn(new StringColumn("abcdef"));
		r.addColumn(new StringColumn("13:50:47"));
		r.addColumn(new StringColumn("2012-12-12 12:12:12"));
		list.add(r);
		return list;
	}

	@Override
	public String getTestPluginName() {
		return "pgwriter";
	}

}
