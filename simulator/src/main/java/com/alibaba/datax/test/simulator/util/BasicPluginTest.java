package com.alibaba.datax.test.simulator.util;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.ClassLoaderSwapper;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public abstract class BasicPluginTest {
    protected static String PLUGIN_PATH = null;

    protected static Configuration PLUGIN_CONF = null;

    protected static String TESTCLASSES_PATH = null;

    @BeforeClass
    public static void mvnCompile() {
        TESTCLASSES_PATH = Thread.currentThread().getClass().getResource("/")
                .getPath();

        ProcessBuilder pb = new ProcessBuilder();
        List<String> commands = new ArrayList<String>();
        commands.add("mvn");
        commands.add("install");
        commands.add("-Dmaven.test.skip=true");
        pb.command(commands);

        pb.directory(new File(TESTCLASSES_PATH).getParentFile().getParentFile());

        try {
            Process p2 = pb.start();
            p2.waitFor();

            String result = IOUtils.toString(p2.getInputStream(), Charset.forName("utf8"));
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }


        System.out.println("mvn install ok.");
    }

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

    public AbstractJobPlugin getPluginMaster(Configuration jobConf,
                                                String pluginName, PluginType pluginType) {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                pluginType, pluginName));

        // PluginLoader.setPluginConfigs(pluginConfigs);
        AbstractJobPlugin master = (AbstractJobPlugin) LoadUtil
                .loadJobPlugin(pluginType, pluginName);

        // 设置reader的jobConfig
        if (pluginType.equals(PluginType.READER)) {
            master.setPluginJobConf(jobConf
                    .getConfiguration(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));
        } else if (pluginType.equals(PluginType.WRITER)) {
            master.setPluginJobConf(jobConf
                    .getConfiguration(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));
        } else {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_NOT_FOUND,
                    "unsupported type:" + pluginType);
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
     * <p/>
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
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_NOT_FOUND,
                    String.format("get Plugin Dir Failed. testclass_path:[%s].",
                            TESTCLASSES_PATH));
        }

        String basicDirPath = targetDir
                + String.format("%sdatax%splugin%s", File.separator, File.separator,
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
