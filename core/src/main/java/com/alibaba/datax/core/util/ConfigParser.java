package com.alibaba.datax.core.util;

import java.io.*;
import java.util.*;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang.StringUtils;

import com.alibaba.datax.common.util.Configuration;

public final class ConfigParser {
    /**
     * 指定Job配置路径，ConfigParser会解析Job、Plugin、Core全部信息，并以Configuration返回
     *
     * */
    public static Configuration parse(final String jobPath) {
		Configuration configuration = ConfigParser.parseJobConfig(jobPath);

		configuration.merge(
				ConfigParser.parseCoreConfig(CoreConstant.DATAX_CONF_PATH),
				false);
		configuration.merge(parsePluginConfig(), false);

		return configuration;
	}

	private static Configuration parseCoreConfig(final String path) {
		return Configuration.from(new File(path));
	}

	public static Configuration parseJobConfig(final String path) {
        Configuration config =
                Configuration.from(new File(path));

        return processSecretKey(config);
	}

    private static Configuration processSecretKey(Configuration config) {
        String keyVersion = config
                .getString(CoreConstant.DATAX_JOB_SETTING_KEYVERSION);
        // 没有设置keyVersion，表示不用解密
        if(StringUtils.isBlank(keyVersion)) {
            return config;
        }

        Map<String, String> versionKeyMap = getPrivateKeyMap();
        String privateKey = versionKeyMap.get(keyVersion);
        // keyVersion要求的私钥没有配置
        if(StringUtils.isBlank(privateKey)) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR,
                    String.format("DataX配置的密钥版本为[%s]，但在系统中没有配置，可能是任务密钥配置错误，也可能是系统维护问题", keyVersion));
        }

        // 对包含*号key解密处理
        for(String key : config.getKeys()) {
            int lastPathIndex = key.lastIndexOf(".") + 1;
            String lastPathKey = key.substring(lastPathIndex);
            if (lastPathKey.length() > 1 && lastPathKey.charAt(0) == '*'
                    && lastPathKey.charAt(1) != '*') {
                Object value = config.get(key);
                if(value instanceof String) {
                    String newKey = key.substring(0, lastPathIndex)
                            + lastPathKey.substring(1);
                    config.set(newKey,
                            SecretUtil.decrypt((String)value, privateKey));
                    config.addSecretKeyPath(newKey);
                    config.remove(key);
                }
            }
        }

        return config;
    }

    private static Map<String, String> getPrivateKeyMap() {
        Map<String, String> versionKeyMap =
                new HashMap<String, String>();
        InputStream secretStream = null;
        try {
            secretStream = new FileInputStream(
                    CoreConstant.DATAX_SECRET_PATH);
        } catch (FileNotFoundException e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR,
                    "DataX配置要求加解密，但无法找到密钥的配置文件");
        }

        Properties properties = new Properties();
        try {
            properties.load(secretStream);
            secretStream.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR, "读取加解密配置文件出错", e);
        }

        String lastKeyVersion = properties.getProperty(
                CoreConstant.LAST_KEYVERSION);
        String lastPublicKey = properties.getProperty(
                CoreConstant.LAST_PUBLICKEY);
        String lastPrivateKey = properties.getProperty(
                CoreConstant.LAST_PRIVATEKEY);
        if(StringUtils.isNotBlank(lastKeyVersion)) {
            if(StringUtils.isBlank(lastPublicKey) ||
                    StringUtils.isBlank(lastPrivateKey)) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.SECRET_ERROR,
                        "DataX配置要求加解密，但上次配置的公私钥对存在为空的情况"
                );
            }

            versionKeyMap.put(lastKeyVersion, lastPrivateKey);
        }

        String currentKeyVersion = properties.getProperty(
                CoreConstant.CURRENT_KEYVERSION);
        String currentPublicKey = properties.getProperty(
                CoreConstant.CURRENT_PUBLICKEY);
        String currentPrivateKey = properties.getProperty(
                CoreConstant.CURRENT_PRIVATEKEY);
        if(StringUtils.isNotBlank(currentKeyVersion)) {
            if(StringUtils.isBlank(currentPublicKey) ||
                    StringUtils.isBlank(currentPrivateKey)) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.SECRET_ERROR,
                        "DataX配置要求加解密，但当前配置的公私钥对存在为空的情况");
            }

            versionKeyMap.put(currentKeyVersion, currentPrivateKey);
        }

        if(versionKeyMap.size() <= 0) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR,
                    "DataX配置要求加解密，但无法找到公私钥");
        }

        return versionKeyMap;
    }

	private static Configuration parsePluginConfig() {
		Configuration configuration = Configuration.newDefault();

		for (final String each : ConfigParser
				.getDirAsList(CoreConstant.DATAX_PLUGIN_READER_HOME)) {
			configuration.merge(
					ConfigParser.parseOnePluginConfig(each, "reader"), true);
		}

		for (final String each : ConfigParser
				.getDirAsList(CoreConstant.DATAX_PLUGIN_WRITER_HOME)) {
			configuration.merge(
					ConfigParser.parseOnePluginConfig(each, "writer"), true);
		}

		return configuration;
	}

	public static Configuration parseOnePluginConfig(final String path,
			final String type) {
		String filePath = path + File.separator + "plugin.json";
		Configuration configuration = Configuration.from(new File(filePath));

		String pluginPath = configuration.getString("path");
		boolean isDefaultPath = StringUtils.isBlank(pluginPath);
		if (isDefaultPath) {
			configuration.set("path", path);
		}

		Configuration result = Configuration.newDefault();

		result.set(
				String.format("plugin.%s.%s", type, configuration.get("name")),
				configuration.getInternal());

		return result;
	}

	private static List<String> getDirAsList(String path) {
		List<String> result = new ArrayList<String>();

		String[] paths = new File(path).list();
		if (null == paths) {
			return result;
		}

		for (final String each : paths) {
			result.add(path + File.separator + each);
		}

		return result;
	}

}
