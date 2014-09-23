package com.alibaba.datax.core.container.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractMasterPlugin;
import com.alibaba.datax.common.plugin.AbstractPlugin;
import com.alibaba.datax.common.plugin.AbstractSlavePlugin;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.runner.AbstractRunner;
import com.alibaba.datax.core.container.runner.RunnerManager;
import com.alibaba.datax.core.util.FrameworkErrorCode;

/**
 * Created by jingxing on 14-8-24.
 * 
 * 插件加载器，大体上分reader、transformer（还未实现）和writer三中插件类型，
 * reader和writer在执行时又可能出现Master和Slave两种运行时（加载的类不同）
 */
public class LoadUtil {
	private static final String pluginTypeNameFormat = "plugin.%s.%s";

	private LoadUtil() {
	}

	private enum ContainerType {
		Master("Master"), Slave("Slave");
		private String type;

		private ContainerType(String type) {
			this.type = type;
		}

		public String value() {
			return type;
		}
	}

	/**
	 * 所有插件配置放置在pluginRegisterCenter中，为区别reader、transformer和writer，还能区别
	 * 具体pluginName，故使用pluginType.pluginName作为key放置在该map中
	 */
	private static Configuration pluginRegisterCenter;

	/**
	 * jarLoader的缓冲
	 */
	private static Map<String, JarLoader> jarLoaderCenter = new HashMap<String, JarLoader>();

	/**
	 * 设置pluginConfigs，方便后面插件来获取
	 * 
	 * @param pluginConfigs
	 */
	public static void bind(Configuration pluginConfigs) {
		pluginRegisterCenter = pluginConfigs;
	}

	private static String generatePluginKey(PluginType pluginType,
			String pluginName) {
		return String.format(pluginTypeNameFormat, pluginType.toString(),
				pluginName);
	}

	private static Configuration getPluginConf(PluginType pluginType,
			String pluginName) {
		Configuration pluginConf = pluginRegisterCenter
				.getConfiguration(generatePluginKey(pluginType, pluginName));

		if (null == pluginConf) {
			throw new DataXException(
					FrameworkErrorCode.PLUGIN_INSTALL_ERROR,
					String.format(
							"System Fatal Error: DataX cannot find configuration for [%s] .",
							pluginName));
		}

		return pluginConf;
	}

	/**
	 * 加载MasterPlugin，reader、writer都可能要加载
	 * 
	 * @param pluginType
	 * @param pluginName
	 * @return
	 */
	public static AbstractMasterPlugin loadMasterPlugin(PluginType pluginType,
			String pluginName) {
		Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(
				pluginType, pluginName, ContainerType.Master);

		try {
			AbstractMasterPlugin masterPlugin = (AbstractMasterPlugin) clazz
					.newInstance();
			masterPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
			return masterPlugin;
		} catch (Exception e) {
			throw new DataXException(
					FrameworkErrorCode.INNER_ERROR,
					String.format(
							"System Fatal Error: DataX cannot find Master plugin %s .",
							pluginName), e);
		}
	}

	/**
	 * 加载SlavePlugin，reader、writer都可能加载
	 * 
	 * @param pluginType
	 * @param pluginName
	 * @return
	 */
	public static AbstractSlavePlugin loadSlavePlugin(PluginType pluginType,
			String pluginName) {
		Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(
				pluginType, pluginName, ContainerType.Slave);

		try {
			AbstractSlavePlugin slavePlugin = (AbstractSlavePlugin) clazz
					.newInstance();
			slavePlugin.setPluginConf(getPluginConf(pluginType, pluginName));
			return slavePlugin;
		} catch (Exception e) {
			throw new DataXException(FrameworkErrorCode.INNER_ERROR,
					String.format("DataX cannot find Slave plugin %s .",
							pluginName), e);
		}
	}

	/**
	 * 根据插件类型、名字和执行时slaveContainerId加载对应运行器
	 * 
	 * @param pluginType
	 * @param pluginName
	 * @param slaveId
	 * @return
	 */
	public static AbstractRunner loadPluginRunner(PluginType pluginType,
			String pluginName, int slaveId) {
		AbstractSlavePlugin slavePlugin = LoadUtil.loadSlavePlugin(pluginType,
				pluginName);

		switch (pluginType) {
		case READER:
			return RunnerManager.newReaderRunner(slavePlugin, slaveId);
		case WRITER:
			return RunnerManager.newWriterRunner(slavePlugin, slaveId);
		default:
			throw new DataXException(
					FrameworkErrorCode.INNER_ERROR,
					String.format(
							"System Fatal Error: Plugin [%s] type must be [reader] or [writer]!",
							pluginName));
		}
	}

	/**
	 * 反射出具体plugin实例
	 * 
	 * @param pluginType
	 * @param pluginName
	 * @param pluginRunType
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static synchronized Class<? extends AbstractPlugin> loadPluginClass(
			PluginType pluginType, String pluginName,
			ContainerType pluginRunType) {
		Configuration pluginConf = getPluginConf(pluginType, pluginName);
		JarLoader jarLoader = LoadUtil.getJarLoader(pluginType, pluginName);
		try {
			return (Class<? extends AbstractPlugin>) jarLoader
					.loadClass(pluginConf.getString("class") + "$"
							+ pluginRunType.value());
		} catch (Exception e) {
			throw new DataXException(FrameworkErrorCode.INNER_ERROR, e);
		}
	}

	public static synchronized JarLoader getJarLoader(PluginType pluginType,
			String pluginName) {
		Configuration pluginConf = getPluginConf(pluginType, pluginName);

		JarLoader jarLoader = jarLoaderCenter.get(generatePluginKey(pluginType,
				pluginName));
		if (null == jarLoader) {
			String pluginPath = pluginConf.getString("path");
			if (StringUtils.isBlank(pluginPath)) {
				throw new DataXException(
						FrameworkErrorCode.INNER_ERROR,
						String.format(
								"System Fatal Error: [%s] plugin [%s] path illegal !",
								pluginType, pluginName));
			}
			jarLoader = new JarLoader(new String[] { pluginPath });
			jarLoaderCenter.put(generatePluginKey(pluginType, pluginName),
					jarLoader);
		}

		return jarLoader;
	}
}
