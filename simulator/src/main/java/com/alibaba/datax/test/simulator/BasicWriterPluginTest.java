package com.alibaba.datax.test.simulator;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.runner.WriterRunner;
import com.alibaba.datax.core.container.util.LoadUtil;
import com.alibaba.datax.core.statistics.metric.MetricManager;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.test.simulator.util.BasicPluginTest;
import com.alibaba.datax.test.simulator.util.RecordReceiverForTest;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class BasicWriterPluginTest extends BasicPluginTest {

    protected Writer.Master writerMaster = null;

    @BeforeClass
    public static void checkPluginPackageDir() {
        PLUGIN_PATH = getPluginDir();
        System.out.println("got plugin dir: " + PLUGIN_PATH);

        File pluginDir = new File(PLUGIN_PATH);

        if (pluginDir.isDirectory()) {
            File[] filesInPluginDir = pluginDir.listFiles();
            Assert.assertEquals(3, filesInPluginDir.length);

            File libsDir = new File(pluginDir + File.separator + "libs");
            File pluginJsonFile = new File(pluginDir + File.separator
                    + "plugin.json");

            String mainJarPath = pluginDir + File.separator
                    + getPluginMainJarName(pluginDir);
            File pluginMainJarFile = new File(mainJarPath);

            Assert.assertTrue("libs should be a dir.", libsDir.isDirectory());
            Assert.assertTrue("libs should not be empty.",
                    libsDir.list().length > 0);

            PLUGIN_CONF = ConfigParser.parseOnePluginConfig(PLUGIN_PATH,
                    "writer");
            Assert.assertTrue("plugin.json file should be a json file.",
                    pluginJsonFile.exists() && pluginJsonFile.isFile()
                            && null != PLUGIN_CONF);

            System.out.println("got pluginConf: " + PLUGIN_CONF.toJSON());

            Assert.assertTrue(mainJarPath + " file not exists.",
                    pluginMainJarFile.exists() && pluginMainJarFile.isFile());

        }
        LoadUtil.bind(PLUGIN_CONF);
        System.out.println("basic env check ok.");
    }

    protected void doWriterTest(String jobName, int mandatoryNumber) {
        Configuration jobConf = getJobConf(TESTCLASSES_PATH + File.separator
                + jobName);

        jobConf.set("jobName", jobName);

        doWriterTest(jobConf, mandatoryNumber);
    }

    protected void doWriterTest(Configuration jobConf, int mandatoryNumber) {
        String pluginName = getTestPluginName();
        writerMaster = (Writer.Master) getPluginMaster(jobConf, pluginName,
                PluginType.WRITER);

        writerMaster.init();
        writerMaster.prepare();
        List<Configuration> jobs = writerMaster.split(mandatoryNumber);

        long channelId = 0;
        long slaveId = 1;
        MetricManager.registerMetric(slaveId, channelId);

        int numThread = jobs.size();
        ExecutorService executor = Executors.newFixedThreadPool(numThread);
        CompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                executor);

        Void v = null;
        for (Configuration job : jobs) {
            WriterRunner writerRunner = getWriterRunner(job, (int) slaveId);

            //TODO 测试框架中，需要对这段逻辑进行完善。
            /**
             * 设置slavePlugin的collector，用来处理脏数据和master/slave通信
             */
//            writerRunner.setSlavePluginCollector(ClassUtil.instantiate(
//                    slaveCollectorClass, AbstractSlavePluginCollector.class,
//                    configuration, this.channel.getChannelMetric(),
//                    PluginType.WRITER));

            completionService.submit(writerRunner, v);
        }

        // just wait all runner finished!
        for (int i = 0; i < numThread; i++) {
            try {
                completionService.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        writerMaster.post();
        writerMaster.destroy();
    }

    private WriterRunner getWriterRunner(Configuration jobConf, int slaveId) {
        WriterRunner writerRunner = (WriterRunner) LoadUtil.loadPluginRunner(
                PluginType.WRITER, getTestPluginName(), slaveId);

        writerRunner.setJobConf(jobConf);
        writerRunner.setRecordReceiver(new RecordReceiverForTest(
                buildDataForWriter()));

        return writerRunner;

    }

    protected abstract List<Record> buildDataForWriter();

    public RecordReceiver createRecordReceiverForTest() {
        return new RecordReceiverForTest(buildDataForWriter());
    }
}
