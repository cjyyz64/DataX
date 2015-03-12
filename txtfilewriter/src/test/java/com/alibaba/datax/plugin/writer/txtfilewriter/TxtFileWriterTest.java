package com.alibaba.datax.plugin.writer.txtfilewriter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import ch.qos.logback.classic.joran.action.ConfigurationAction;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public class TxtFileWriterTest  extends BasicWriterPluginTest{
	
	@Test
	public void testBasic0(){
		
		super.doWriterTest("basico0.json", 1);
	}

	@Override
	protected List<Record> buildDataForWriter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getTestPluginName() {
		// TODO Auto-generated method stub
		return null;
	}
}
