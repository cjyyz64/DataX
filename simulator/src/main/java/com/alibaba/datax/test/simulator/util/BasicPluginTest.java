package com.alibaba.datax.test.simulator.util;

import java.io.File;
import java.io.FilenameFilter;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractMasterPlugin;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.util.ClassLoaderSwapper;
import com.alibaba.datax.core.container.util.LoadUtil;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.FrameworkErrorCode;

public abstract class BasicPluginTest {
	protected static String PLUGIN_PATH = null;

	protected static Configuration PLUGIN_CONF = null;

	protected static String TESTCLASSES_PATH = null;

	// 其实在这里测试的时候，不要swapper 也可以。
	private ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper
			.newCurrentThreadClassLoaderSwapper();

	protected static String getPluginMainJarName(File pluginDir) {
		Assert.assertNotNull(pluginDir);
		Assert.assertTrue(pluginDir.isDirectory());

		String[] files = pluginDir.list(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.contains(".jar");
			}
		});

		Assert.assertTrue("plugin main jar should have only one jar.",
				files.length == 1);
		return files[0];
	}

	public AbstractMasterPlugin getPluginMaster(Configuration jobConf,
			String pluginName, PluginType pluginType) {
		classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
				pluginType, pluginName));

		// PluginLoader.setPluginConfigs(pluginConfigs);
		AbstractMasterPlugin master = (AbstractMasterPlugin) LoadUtil
				.loadMasterPlugin(pluginType, pluginName);

		// 设置reader的jobConfig
		if (pluginType.equals(PluginType.READER)) {
			master.setPluginJobConf(jobConf
					.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));
		} else if (pluginType.equals(PluginType.WRITER)) {
			master.setPluginJobConf(jobConf
					.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));
		} else {
			throw new DataXException(FrameworkErrorCode.PLUGIN_NOT_FOUND,
					"unsupport type:" + pluginType);
		}

		classLoaderSwapper.restoreCurrentThreadClassLoader();
		return master;
	}

	public static Configuration getJobConf(final String jobConfPath) {
		return ConfigParser.parseJobConfig(jobConfPath);
	}

	/**
	 * 从如下目录结构中的test-classes
	 * 目录开始，解析得到插件打包后的具体路径：/someBasciPath/datax/plugin/reader/mysqlreader
	 * 
	 * <pre>
	 * test-classes
	 * ├── basic.json
	 * └── com
	 *     └── alibaba
	 *         └── datax
	 *             └── plugin
	 *                 └── reader
	 *                     └── mysqlreader
	 *                         └── MysqlReaderTest.class
	 * datax
	 * └── plugin  
	 *     └── reader	(对应代码中的：d1)
	 *         └── mysqlreader	(对应代码中的：d2)
	 *             ├── libs
	 *             │   ├── datax-common-0.0.1-SNAPSHOT.jar
	 *             │   ├── datax-core-0.0.1-SNAPSHOT.jar
	 *             ├── mysqlreader-0.0.1-SNAPSHOT.jar
	 *             └── plugin.json
	 * 
	 * </pre>
	 */
	protected static String getPluginDir() {
		TESTCLASSES_PATH = Thread.currentThread().getClass().getResource("/")
				.getPath();

		String targetDir = new File(TESTCLASSES_PATH).getParent();
		if (StringUtils.isBlank(targetDir)) {
			throw new DataXException(FrameworkErrorCode.PLUGIN_NOT_FOUND,
					String.format("ge Plugin Dir Failed. testclass_path:[%s].",
							TESTCLASSES_PATH));
		}

		String basicDirPath = targetDir
				+ String.format("datax%splugin%s", File.separator,
						File.separator);
		String pluginDirPath = null;
		try {

			File d1 = new File(basicDirPath).listFiles()[0];

			File d2 = d1.listFiles()[0];

			pluginDirPath = d2.getAbsolutePath() + File.separator;
		} catch (Exception e) {
			Assert.assertTrue(
					"should run this test after you have package your plugin and create target dir and your pluin should have dir like plugin/reader/mysqlreader",
					false);
		}

		return pluginDirPath;
	}

	protected abstract String getTestPluginName();
}
