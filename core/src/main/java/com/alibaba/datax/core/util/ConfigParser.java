package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.methods.HttpGet;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;

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

    private static String getJobContent(String jobResource) {
        String jobContent;

        boolean isJobResourceFromHttp = jobResource.trim().toLowerCase().startsWith("http");


        if (isJobResourceFromHttp) {
            //设置httpclient的 HTTP_TIMEOUT_INMILLIONSECONDS
            Configuration coreConfig = ConfigParser.parseCoreConfig(CoreConstant.DATAX_CONF_PATH);
            int httpTimeOutInMillionSeconds = coreConfig.getInt(
                    CoreConstant.DATAX_CORE_DATAXSERVER_TIMEOUT, 5000);
            HttpClientUtil.setHttpTimeoutInMillionSeconds(httpTimeOutInMillionSeconds);

            HttpClientUtil httpClientUtil = new HttpClientUtil();
            try {
                URL url = new URL(jobResource);
                HttpGet httpGet = HttpClientUtil.getGetRequest();
                httpGet.setURI(url.toURI());

                jobContent = httpClientUtil.executeAndGetWithRetry(httpGet, 6, 1000l);
            } catch (Exception e) {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "获取作业配置信息失败:" + jobResource, e);
            }
        } else {
            // jobResource 是本地文件绝对路径
            try {
                jobContent = FileUtils.readFileToString(new File(jobResource));
            } catch (IOException e) {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "获取作业配置信息失败:" + jobResource, e);
            }
        }

        if (jobContent == null) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "获取作业配置信息失败:" + jobResource);
        }
        return jobContent;
    }

    private static Configuration parsePluginConfig() {
        Configuration configuration = Configuration.newDefault();

        Set<String> pluginSet = new HashSet<String>();
        for (final String each : ConfigParser
                .getDirAsList(CoreConstant.DATAX_PLUGIN_READER_HOME)) {
            Configuration eachReaderConfig = ConfigParser.parseOnePluginConfig(each, "reader", pluginSet);
            configuration.merge(
                    eachReaderConfig, true);
        }

        for (final String each : ConfigParser
                .getDirAsList(CoreConstant.DATAX_PLUGIN_WRITER_HOME)) {
            Configuration eachWriterConfig = ConfigParser.parseOnePluginConfig(each, "writer", pluginSet);
            configuration.merge(
                    eachWriterConfig, true);
        }

        return configuration;
    }


    public static Configuration parseOnePluginConfig(final String path,
                                                     final String type,
                                                     Set<String> pluginSet) {
        String filePath = path + File.separator + "plugin.json";
        Configuration configuration = Configuration.from(new File(filePath));

        String pluginPath = configuration.getString("path");
        String pluginName = configuration.getString("name");
        if(!pluginSet.contains(pluginName)) {
            pluginSet.add(pluginName);
        } else {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_INIT_ERROR, "插件加载失败,存在重复插件:" + filePath);
        }

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
