package com.alibaba.datax.core.container;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.util.LoadUtil;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.scheduler.distribute.DistributeScheduler;
import com.alibaba.datax.core.scheduler.standalone.StandAloneScheduler;
import com.alibaba.datax.core.statistics.metric.Metric;
import com.alibaba.datax.core.statistics.metric.MetricManager;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.CoreConstant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jingxing on 14-9-2.
 */
public class MasterContainerTest extends CaseInitializer {
	private Configuration configuration;

	@Before
	public void setUp() {
		String path = MasterContainerTest.class.getClassLoader()
				.getResource(".").getFile();

		this.configuration = ConfigParser.parse(path + File.separator
				+ "all.json");
		LoadUtil.bind(this.configuration);
	}

	/**
	 * standalone模式下点对点跑完全部流程
	 */
	@Test
	public void testStart() {
		MasterContainer masterContainer = new MasterContainer(
				this.configuration);
		masterContainer.start();
	}

	@Test(expected = Exception.class)
	public void testStartException() {
		this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_MASTER_ID, -2);
		this.configuration.set(CoreConstant.DATAX_CORE_SCHEDULER_CLASS,
				DistributeScheduler.class.getName());
		MasterContainer masterContainer = new MasterContainer(
				this.configuration);
		masterContainer.start();
	}

	@Test
	public void testInitNormal() throws Exception {
		this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_MASTER_ID, -2);
		this.configuration.set(CoreConstant.DATAX_CORE_SCHEDULER_CLASS,
				StandAloneScheduler.class.getName());
		MasterContainer masterContainer = new MasterContainer(
				this.configuration);

		Method initMethod = masterContainer.getClass()
				.getDeclaredMethod("init");
		initMethod.setAccessible(true);
		initMethod.invoke(masterContainer, new Object[] {});
		Assert.assertEquals("default master id = 0", 0l, this.configuration
				.getLong(CoreConstant.DATAX_CORE_CONTAINER_MASTER_ID)
				.longValue());
	}

	@Test(expected = Exception.class)
	public void testInitException() throws Exception {
		this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_MASTER_ID, -2);
		this.configuration.set(CoreConstant.DATAX_CORE_SCHEDULER_CLASS,
				DistributeScheduler.class.getName());
		MasterContainer masterContainer = new MasterContainer(
				this.configuration);
		Method initMethod = masterContainer.getClass()
				.getDeclaredMethod("init");
		initMethod.setAccessible(true);
		initMethod.invoke(masterContainer, new Object[] {});
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMergeReaderAndWriterSlicesConfigs() throws Exception {
		MasterContainer masterContainer = new MasterContainer(
				this.configuration);
		Method initMethod = masterContainer.getClass()
				.getDeclaredMethod("init");
		initMethod.setAccessible(true);
		initMethod.invoke(masterContainer, new Object[] {});
		initMethod.setAccessible(false);

		int splitNumber = 100;
		List<Configuration> readerSplitConfigurations = new ArrayList<Configuration>();
		List<Configuration> writerSplitConfigurations = new ArrayList<Configuration>();
		for (int i = 0; i < splitNumber; i++) {
			Configuration readerOneConfig = Configuration.newDefault();
			List<String> jdbcUrlArray = new ArrayList<String>();
			jdbcUrlArray.add(String.format(
					"jdbc:mysql://localhost:3305/db%04d", i));
			readerOneConfig.set("jdbcUrl", jdbcUrlArray);

			List<String> tableArray = new ArrayList<String>();
			tableArray.add(String.format("jingxing_%04d", i));
			readerOneConfig.set("table", tableArray);

			readerSplitConfigurations.add(readerOneConfig);

			Configuration writerOneConfig = Configuration.newDefault();
			List<String> odpsUrlArray = new ArrayList<String>();
			odpsUrlArray.add(String.format("odps://localhost:3305/db%04d", i));
			writerOneConfig.set("jdbcUrl", odpsUrlArray);

			List<String> odpsTableArray = new ArrayList<String>();
			odpsTableArray.add(String.format("jingxing_%04d", i));
			writerOneConfig.set("table", odpsTableArray);

			writerSplitConfigurations.add(writerOneConfig);
		}

		initMethod = masterContainer.getClass().getDeclaredMethod(
				"mergeReaderAndWriterSlicesConfigs", List.class, List.class);
		initMethod.setAccessible(true);

		List<Configuration> mergedConfigs = (List<Configuration>) initMethod
				.invoke(masterContainer, readerSplitConfigurations, writerSplitConfigurations);

		Assert.assertEquals("merge number equals to split number", splitNumber,
				mergedConfigs.size());
		for (Configuration sliceConfig : mergedConfigs) {
			Assert.assertNotNull("reader name not null",
					sliceConfig.getString(CoreConstant.JOB_READER_NAME));
			Assert.assertNotNull("reader name not null",
					sliceConfig.getString(CoreConstant.JOB_READER_PARAMETER));
			Assert.assertNotNull("reader name not null",
					sliceConfig.getString(CoreConstant.JOB_WRITER_NAME));
			Assert.assertNotNull("reader name not null",
					sliceConfig.getString(CoreConstant.JOB_WRITER_PARAMETER));
			Assert.assertTrue("has slice id",
					sliceConfig.getInt(CoreConstant.JOB_SLICEID) >= 0);
		}
	}

	@Test(expected = Exception.class)
	public void testMergeReaderAndWriterSlicesConfigsException()
			throws Exception {
		MasterContainer masterContainer = new MasterContainer(
				this.configuration);
		Method initMethod = masterContainer.getClass()
				.getDeclaredMethod("init");
		initMethod.setAccessible(true);
		initMethod.invoke(masterContainer, new Object[] {});
		initMethod.setAccessible(false);

		int readerSplitNumber = 100;
		int writerSplitNumber = readerSplitNumber + 1;
		List<Configuration> readerSplitConfigurations = new ArrayList<Configuration>();
		List<Configuration> writerSplitConfigurations = new ArrayList<Configuration>();
		for (int i = 0; i < readerSplitNumber; i++) {
			Configuration readerOneConfig = Configuration.newDefault();
			readerSplitConfigurations.add(readerOneConfig);
		}
		for (int i = 0; i < writerSplitNumber; i++) {
			Configuration readerOneConfig = Configuration.newDefault();
			writerSplitConfigurations.add(readerOneConfig);
		}

		initMethod = masterContainer.getClass().getDeclaredMethod(
				"mergeReaderAndWriterSlicesConfigs", List.class, List.class);
		initMethod.setAccessible(true);
		initMethod.invoke(masterContainer, readerSplitConfigurations, writerSplitConfigurations);
	}

	@Test
	public void testDistributeSlicesToSlaveContainer() throws Exception {
		distributeSlicesToSlaveContainerTest(333, 7);

		distributeSlicesToSlaveContainerTest(6, 7);
		distributeSlicesToSlaveContainerTest(7, 7);
		distributeSlicesToSlaveContainerTest(8, 7);

		distributeSlicesToSlaveContainerTest(1, 1);
		distributeSlicesToSlaveContainerTest(2, 1);
		distributeSlicesToSlaveContainerTest(1, 2);

		distributeSlicesToSlaveContainerTest(1, 1025);
		distributeSlicesToSlaveContainerTest(1024, 1025);
	}

	/**
	 * 分发测试函数，可根据不同的通道数、每个slave平均包括的channel数得到最优的分发结果
	 * 注意：默认的slices是采用faker里切分出的1024个slices
	 * 
	 * @param channelNumber
	 * @param channelsPerSlaveContainer
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private void distributeSlicesToSlaveContainerTest(int channelNumber,
			int channelsPerSlaveContainer) throws Exception {
		MasterContainer masterContainer = new MasterContainer(
				this.configuration);
		Method initMethod = masterContainer.getClass()
				.getDeclaredMethod("init");
		initMethod.setAccessible(true);
		initMethod.invoke(masterContainer, new Object[] {});
		initMethod.setAccessible(false);

		initMethod = masterContainer.getClass().getDeclaredMethod("split");
		initMethod.setAccessible(true);
		initMethod.invoke(masterContainer, new Object[] {});
		initMethod.setAccessible(false);

		int sliceNumber = this.configuration.getListConfiguration(
				CoreConstant.DATAX_JOB_CONTENT).size();
		int averSlicesPerChannel = sliceNumber / channelNumber;

		initMethod = masterContainer.getClass().getDeclaredMethod(
				"distributeSlicesToSlaveContainer", int.class, int.class,
				int.class);
		initMethod.setAccessible(true);
		List<Configuration> slaveConfigs = (List<Configuration>) initMethod
				.invoke(masterContainer, averSlicesPerChannel,
                        channelNumber, channelsPerSlaveContainer);
		initMethod.setAccessible(false);

		Assert.assertEquals("slave size check", channelNumber
				/ channelsPerSlaveContainer
				+ (channelNumber % channelsPerSlaveContainer > 0 ? 1 : 0),
				slaveConfigs.size());
		int sumSlices = 0;
		for (Configuration slaveConfig : slaveConfigs) {
			Assert.assertNotNull("have set slaveId", slaveConfig
					.getInt(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID));
			int channelNo = slaveConfig
					.getInt(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_CHANNEL);
			Assert.assertNotNull("have set slave channel number", channelNo);
			int slaveSlices = slaveConfig.getListConfiguration(
					CoreConstant.DATAX_JOB_CONTENT).size();
			sumSlices += slaveSlices;
			Assert.assertTrue("slave has average slices", slaveSlices
					/ channelNo == averSlicesPerChannel);
		}

		Assert.assertEquals("slices equal to split sum", sliceNumber, sumSlices);
	}

    @Test
    public void testErrorLimitIgnoreCheck() throws Exception {
        this.configuration.set(CoreConstant.DATAX_JOB_SETTING_ERRORLIMIT, -1);
        MasterContainer masterContainer = new MasterContainer(
                this.configuration);

        MetricManager.registerMetric(0, 0);
        Metric metric = MetricManager.getChannelMetric(0, 0);
        metric.setReadSucceedRecords(100);
        metric.setReadFailedRecords(0);
        metric.setWriteReceivedRecords(100);
        metric.setWriteFailedRecords(0);

        Method initMethod = masterContainer.getClass()
                .getDeclaredMethod("checkLimit");
        initMethod.setAccessible(true);
        initMethod.invoke(masterContainer, new Object[] {});
        initMethod.setAccessible(false);
    }

    @Test(expected = Exception.class)
    public void testErrorLimitPercentCheck() throws Exception {
//        this.configuration.set(CoreConstant.DATAX_JOB_SETTING_ERRORLIMIT, 0.1);
//        this.configuration.set(CoreConstant.DATAX_JOB_SETTING_ERRORLIMIT_RECORD, null);
        this.configuration.remove(CoreConstant.DATAX_JOB_SETTING_ERRORLIMIT_RECORD);
        this.configuration.set(CoreConstant.DATAX_JOB_SETTING_ERRORLIMIT_PERCENT, 0.1);
        MasterContainer masterContainer = new MasterContainer(
                this.configuration);

        MetricManager.registerMetric(0, 0);
        Metric metric = MetricManager.getChannelMetric(0, 0);
        MetricManager.reportSlaveMetric(0, metric);
        metric.setReadSucceedRecords(100);
        metric.setReadFailedRecords(0);
        metric.setWriteReceivedRecords(80);
        metric.setWriteFailedRecords(20);

        Method initMethod = masterContainer.getClass()
                .getDeclaredMethod("checkLimit");
        initMethod.setAccessible(true);
        initMethod.invoke(masterContainer);
        initMethod.setAccessible(false);
    }

    @Test(expected = Exception.class)
    public void testErrorLimitCountCheck() throws Exception {
        this.configuration.remove(CoreConstant.DATAX_JOB_SETTING_ERRORLIMIT_PERCENT);
        this.configuration.set(CoreConstant.DATAX_JOB_SETTING_ERRORLIMIT_RECORD, 1);
        MasterContainer masterContainer = new MasterContainer(
                this.configuration);

        MetricManager.registerMetric(0, 0);
        Metric metric = MetricManager.getChannelMetric(0, 0);
        MetricManager.reportSlaveMetric(0, metric);
        metric.setReadSucceedRecords(100);
        metric.setReadFailedRecords(0);
        metric.setWriteReceivedRecords(98);
        metric.setWriteFailedRecords(2);

        Method initMethod = masterContainer.getClass()
                .getDeclaredMethod("checkLimit");
        initMethod.setAccessible(true);
        initMethod.invoke(masterContainer);
        initMethod.setAccessible(false);
    }
}
