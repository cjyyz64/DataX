package com.alibaba.datax.test.simulator;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.runner.ReaderRunner;
import com.alibaba.datax.core.container.util.LoadUtil;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.test.simulator.util.BasicPluginTest;
import com.alibaba.datax.test.simulator.util.RecordSenderForTest;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.File;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class BasicReaderPluginTest extends BasicPluginTest {
    protected Reader.Job jobReader = null;

    @BeforeClass
    public static void checkPluginPackageDir() {
        PLUGIN_PATH = getPluginDir();
        System.out.println("got plugin dir: " + PLUGIN_PATH);

        File pluginDir = new File(PLUGIN_PATH);

        if (pluginDir.isDirectory()) {
            File[] filesInPluginDir = pluginDir.listFiles();
            Assert.assertTrue(
                    "plugin at least should contain libs,plugin.json, plungin.xx.jar.",
                    filesInPluginDir.length >= 3);

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
                    "reader");
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

    protected List<Configuration> doReaderTest(String jobName,
                                               int adviceSplitNumber, List<Record> noteRecordForTest) {
        Configuration jobConf = super.getJobConf(TESTCLASSES_PATH
                + File.separator + jobName);

        jobConf.set("jobName", jobName);

        return doReaderTest(jobConf, adviceSplitNumber, noteRecordForTest);
    }

    protected List<Configuration> doReaderTest(Configuration jobConf,
                                               int adviceSplitNumber, List<Record> noteRecordForTest) {
        String pluginName = getTestPluginName();
        jobReader = (Reader.Job) super.getPluginMaster(jobConf,
                pluginName, PluginType.READER);

        jobReader.init();
        jobReader.prepare();
        List<Configuration> jobs = jobReader.split(adviceSplitNumber);

        if (null == jobs || jobs.isEmpty()) {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    "split job failed.");
        }

        long channelId = 0;
        long slaveId = 1;
//        MetricManager.registerMetric(slaveId, channelId);

        int numThread = jobs.size();
        ExecutorService executor = Executors.newFixedThreadPool(numThread);
        CompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                executor);

        Void v = null;
        String outputNameTemplate = "%s-%s-%d-%s";
        String tempOutputName = null;

        List<List<Record>> allTempRecordForTest = new ArrayList<List<Record>>();

        for (int i = 0, len = numThread; i < len; i++) {
            allTempRecordForTest.add(new ArrayList<Record>());
        }

        for (int i = 0, len = jobs.size(); i < len; i++) {
            tempOutputName = String.format(outputNameTemplate, Thread
                            .currentThread().getClass().getResource("/").getPath()
                            + (jobConf.getString("jobName") == null ? String.valueOf(i)
                            : jobConf.getString("jobName")), "readerRunner", i,
                    getTestPluginName());

            OutputStream output = buildDataOutput(tempOutputName);

            ReaderRunner readerRunner = getReaderRunner(jobs.get(i),
                    (int) slaveId, output == null ? null : new PrintWriter(
                            output), allTempRecordForTest.get(i));
            completionService.submit(readerRunner, v);
        }

        // just wait all runner finished!
        for (int i = 0; i < numThread; i++) {
            try {
                completionService.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (List<Record> tempList : allTempRecordForTest) {
            noteRecordForTest.addAll(tempList);
        }
        jobReader.post();
        jobReader.destroy();

        return jobs;
    }

    private ReaderRunner getReaderRunner(Configuration jobConf, int slaveId,
                                         PrintWriter printWriter, List<Record> noteRecordForTest) {
        ReaderRunner readerRunner = (ReaderRunner) LoadUtil.loadPluginRunner(
                PluginType.READER, getTestPluginName());

        readerRunner.setJobConf(jobConf);
        readerRunner.setRecordSender(new RecordSenderForTest(printWriter,
                noteRecordForTest));

        return readerRunner;

    }

    protected abstract OutputStream buildDataOutput(String optionalOutputName);

}
