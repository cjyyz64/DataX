package com.alibaba.datax.core;

import java.io.File;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.AbstractContainer;
import com.alibaba.datax.core.container.util.LoadUtil;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.Status;

/**
 * Engine是DataX入口类，该类负责初始化Master或者Slave的运行容器，并运行插件的Master或者Slave逻辑
 * 
 * */
public class Engine {
	private static final Logger LOG = LoggerFactory.getLogger(Engine.class);

	/* check job model (master/slave) first */
	public void start(Configuration allConf) {

		// 绑定column转换信息
		ColumnCast.bind(allConf);

		/**
		 * 初始化PluginLoader，可以获取各种插件配置
		 */
		LoadUtil.bind(allConf);

		boolean isMaster = !("slave".equalsIgnoreCase(allConf
				.getString(CoreConstant.DATAX_CORE_CONTAINER_MODEL)));

		AbstractContainer container;
		if (isMaster) {
			container = ClassUtil.instantiate(allConf
					.getString(CoreConstant.DATAX_CORE_CONTAINER_MASTER_CLASS),
					AbstractContainer.class, allConf);
		} else {
			container = ClassUtil.instantiate(allConf
					.getString(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_CLASS),
					AbstractContainer.class, allConf);
		}

		container.start();
	}

	/**
	 * configure log environment.
	 * 
	 * @param jobId
	 *            DataX job id.
	 * 
	 * */
	private static void confLog(String jobId) throws Exception {
		java.util.Calendar c = java.util.Calendar.getInstance();
		java.text.SimpleDateFormat f = new java.text.SimpleDateFormat(
				"yyyyMMddHHmmss");
		System.setProperty("log.file", jobId + "." + f.format(c.getTime()));

		ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
		LoggerContext loggerContext = (LoggerContext) loggerFactory;
		loggerContext.reset();
		JoranConfigurator configurator = new JoranConfigurator();
		configurator.setContext(loggerContext);
		configurator.doConfigure(new File(CoreConstant.DATAX_CONF_LOG_PATH)
				.toURI().toURL());
	}

	private static String copyRight() {
		String title = "\nDataX, From Alibaba ! \nCopyright (C) 2010-2014, Alibaba Group. All Rights Reserved.\n";
		return title;
	}

	// 注意屏蔽敏感信息
	private static String filterJobConfiguration(
			final Configuration configuration) {
		Configuration jobConf = configuration.getConfiguration("job.content")
				.clone();

		Set<String> keys = jobConf.getKeys();
		for (final String key : keys) {
			boolean isSensitive = StringUtils.endsWithIgnoreCase(key,
					"password")
					|| StringUtils.endsWithIgnoreCase(key, "accessKey");
			if (isSensitive && jobConf.get(key) instanceof String) {
				jobConf.set(key, jobConf.getString(key).replaceAll(".", "*"));
			}
		}

		return jobConf.beautify();
	}

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption("job", true, "Job Config .");
		options.addOption("help", false, "Show help .");

		BasicParser parser = new BasicParser();
		CommandLine cl = parser.parse(options, args);

		if (cl.hasOption("help")) {
			HelpFormatter f = new HelpFormatter();
			f.printHelp("OptionsTip", options);
			System.exit(Status.SUCCESS.value());
		}

		// TODO: add help info.
		if (!cl.hasOption("job")) {
			System.err.printf(String.format("Usage: %s/bin/datax.py job.xml .",
					CoreConstant.DATAX_HOME));
			System.exit(Status.FAIL.value());
		}
		String jobPath = cl.getOptionValue("job");
		Configuration configuration = ConfigParser.parse(jobPath);

		String jobId = configuration.getString(
				CoreConstant.DATAX_CORE_CONTAINER_MASTER_ID, UUID.randomUUID()
						.toString());
		Engine.confLog(jobId);

		LOG.info("\n" + Engine.copyRight());

		LOG.info("\n" + Engine.filterJobConfiguration(configuration) + "\n");

		LOG.debug(configuration.toJSON());

		ConfigurationValidate.doValidate(configuration);
		Engine engine = new Engine();
		try {
			engine.start(configuration);
			System.exit(Status.SUCCESS.value());
		} catch (Throwable e) {
			System.err.println(e.getStackTrace());
			System.exit(Status.FAIL.value());
		}

		System.exit(Status.FAIL.value());
	}

}
