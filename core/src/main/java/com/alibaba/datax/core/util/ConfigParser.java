package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.client.methods.HttpGet;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public final class ConfigParser {
    /**
     * 指定Job配置路径，ConfigParser会解析Job、Plugin、Core全部信息，并以Configuration返回
     */
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
        String jobContent = getJobContent(path);
        Configuration config = Configuration.from(jobContent);

        return SecretUtil.decryptSecretKey(config);
    }

    /**
     * jobResource 如果是本地文件，必须以 file 开头作为其 protocol
     */
    private static String getJobContent(String jobResource) {
        String jobContent = null;

        try {
            URL url = new URL(jobResource);
            String protocol = url.getProtocol();
            Validate.notNull(protocol);

            if (protocol.toLowerCase().contains("http")) {
                HttpGet httpGet = HttpClientUtil.getGetRequest();
                httpGet.setURI(url.toURI());
                jobContent = HttpClientUtil.getHttpClientUtil().executeAndGetWithRetry(httpGet, 3, 1000l);
            } else if (protocol.toLowerCase().contains("file")) {
                jobContent = IOUtils.toString(new FileInputStream(url.getFile()));
            } else {
                throw new Exception("unsupported protocol:" + protocol);
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "获取作业配置信息失败:" + jobResource, e);
        }

        return jobContent;
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
