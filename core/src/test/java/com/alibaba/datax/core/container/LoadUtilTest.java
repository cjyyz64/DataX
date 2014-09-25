package com.alibaba.datax.core.container;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.plugin.AbstractMasterPlugin;
import com.alibaba.datax.common.plugin.AbstractSlavePlugin;
import com.alibaba.datax.core.container.util.LoadUtil;
import com.alibaba.datax.core.scaffold.ConfigurationProducer;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.fastjson.JSON;

public class LoadUtilTest extends CaseInitializer {

	@Test
	public void test() {
		LoadUtil.bind(ConfigurationProducer.produce());
		AbstractMasterPlugin master = LoadUtil.loadMasterPlugin(
                PluginType.READER, "fakereader");
		System.out.println(JSON.toJSONString(master));
		Assert.assertTrue(master.getName().equals("fakereader"));

		AbstractSlavePlugin slave = LoadUtil.loadSlavePlugin(
                PluginType.READER, "fakereader");
		System.out.println(JSON.toJSONString(slave));
		Assert.assertTrue(slave.getName().equals("fakereader"));

	}

}
