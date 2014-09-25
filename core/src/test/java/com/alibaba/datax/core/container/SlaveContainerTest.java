package com.alibaba.datax.core.container;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.runner.AbstractRunner;
import com.alibaba.datax.core.container.runner.RunnerManager;
import com.alibaba.datax.core.container.util.LoadUtil;
import com.alibaba.datax.core.faker.FakeExceptionReader;
import com.alibaba.datax.core.faker.FakeExceptionWriter;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.statistics.metric.Metric;
import com.alibaba.datax.core.statistics.metric.MetricManager;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.Status;

/**
 * Created by jingxing on 14-9-4.
 */
public class SlaveContainerTest extends CaseInitializer {
	private Configuration configuration;

	@Before
	public void setUp() {
		String path = SlaveContainerTest.class.getClassLoader()
				.getResource(".").getFile();

		this.configuration = ConfigParser.parse(path + File.separator
				+ "all.json");
		LoadUtil.bind(this.configuration);

		int channelNumber = 5;
		int jobNumber = channelNumber + 3;
		this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_MASTER_ID, 0);
		this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID, 1);
		this.configuration.set(
				CoreConstant.DATAX_CORE_CONTAINER_SLAVE_SLEEPINTERVAL, 200);
		this.configuration.set(
				CoreConstant.DATAX_CORE_CONTAINER_SLAVE_REPORTINTERVAL, 1000);
		this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_CHANNEL,
				channelNumber);
		Configuration jobContent = this.configuration.getListConfiguration(
				CoreConstant.DATAX_JOB_CONTENT).get(0);
		List<Configuration> jobContents = new ArrayList<Configuration>();
		for (int i = 0; i < jobNumber; i++) {
			Configuration newJobContent = jobContent.clone();
			newJobContent.set(CoreConstant.JOB_SLICEID, i);
			jobContents.add(newJobContent);
		}
		this.configuration.set(CoreConstant.DATAX_JOB_CONTENT, jobContents);
	}

	@Test
	public void testStart() throws InterruptedException {
		SlaveContainer slaveContainer = new SlaveContainer(this.configuration);
		slaveContainer.start();

		List<AbstractRunner> runners = RunnerManager.getRunners();
		while (true) {
			boolean isAllFinished = true;
			boolean isFailed = false;
			for (AbstractRunner runner : runners) {
				if (runner.getRunnerStatus().isFail()) {
					isFailed = true;
					break;
				}

				if (!runner.getRunnerStatus().isSuccess()) {
					isAllFinished = false;
					break;
				}
			}

			if (isFailed || isAllFinished) {
				break;
			}
		}

		for (;;) {
			Metric metric = MetricManager.getMasterMetric();
			if (!metric.getStatus().equals(Status.RUN)) {
				break;
			}
			Thread.sleep(200);
		}

		List<String> messages = null;

		messages = MetricManager.getMasterMetric().getMessage("bazhen-reader");
		Assert.assertTrue(!messages.isEmpty());

		messages = MetricManager.getMasterMetric().getMessage("bazhen-writer");
		Assert.assertTrue(!messages.isEmpty());

		messages = MetricManager.getMasterMetric().getMessage("bazhen");
		Assert.assertNull(messages);

		Map<Long, Metric> slaveMetrics = MetricManager.getAllSlaveMetric();

		for (final Long key : slaveMetrics.keySet()) {
			Assert.assertTrue(!slaveMetrics.get(key)
					.getMessage("bazhen-reader").isEmpty());
			Assert.assertTrue(!slaveMetrics.get(key)
					.getMessage("bazhen-writer").isEmpty());
			Assert.assertTrue(slaveMetrics.get(key).getMessage("bazhen") == null);
		}

		Status status = MetricManager.getSlaveStatusBySlaveId(1);

		Assert.assertTrue("task finished", status.equals(Status.SUCCESS));
	}

	@Test(expected = RuntimeException.class)
	public void testReaderException() {
		this.configuration.set("plugin.reader.fakereader.class",
				FakeExceptionReader.class.getCanonicalName());
		SlaveContainer slaveContainer = new SlaveContainer(this.configuration);
		slaveContainer.start();
	}

	@Test(expected = RuntimeException.class)
	public void testWriterException() {
		this.configuration.set("plugin.writer.fakewriter.class",
				FakeExceptionWriter.class.getName());
		SlaveContainer slaveContainer = new SlaveContainer(this.configuration);
		slaveContainer.start();
	}
}
