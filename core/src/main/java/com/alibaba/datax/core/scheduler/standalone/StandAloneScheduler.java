package com.alibaba.datax.core.scheduler.standalone;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.AbstractContainer;
import com.alibaba.datax.core.scheduler.ErrorRecordLimit;
import com.alibaba.datax.core.scheduler.Scheduler;
import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.metric.Metric;
import com.alibaba.datax.core.statistics.metric.MetricManager;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.Status;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 * 该类是工具包模式下的调度类，它是通过master起多线程来运行slave作业的，除了调度方式和其他模式不一致外，
 * 它的状态汇报也比较特殊，由于在同一个进程中，状态和统计可以在一个全局单粒中提交，master也可以从该单粒中
 * 获取这些信息。但整个架构还是和其他模式保持一致性
 */
public class StandAloneScheduler implements Scheduler {
	private static final Logger LOG = LoggerFactory
			.getLogger(StandAloneScheduler.class);

	private List<SlaveContainerRunner> slaveContainerRunnerList = new ArrayList<SlaveContainerRunner>();

	@Override
	public void schedule(List<Configuration> configurations,
			ContainerCollector frameworkCollector) {
		Validate.notNull(configurations,
				"local scheduler configurations can not be null");

		int masterReportIntervalInMillSec = configurations.get(0).getInt(
				CoreConstant.DATAX_CORE_CONTAINER_MASTER_REPORTINTERVAL, 10000);

		ErrorRecordLimit.setErrorRecordsLimit(configurations.get(0).getDouble(
				CoreConstant.DATAX_JOB_SETTING_ERRORLIMIT, 0.0));

		ExecutorService slaveExecutorService = Executors
				.newFixedThreadPool(configurations.size());

		/**
		 * 完成一个slice算一个stage，所以这里求所有slices的和
		 */
		int totalSlices = 0;
		for (Configuration slaveConfiguration : configurations) {
			SlaveContainerRunner slaveContainerRunner = newSlaveContainerRunner(slaveConfiguration);
			totalSlices += slaveConfiguration.getListConfiguration(
					CoreConstant.DATAX_JOB_CONTENT).size();
			slaveExecutorService.execute(slaveContainerRunner);
			slaveContainerRunnerList.add(slaveContainerRunner);
		}
		slaveExecutorService.shutdown();

		Metric lastMetric = new Metric();
		lastMetric.setTimeStamp(0);
		try {
			do {
				Metric nowMetric = frameworkCollector.collect();
				nowMetric.setTimeStamp(System.currentTimeMillis());
				LOG.debug(nowMetric.toString());

				if (nowMetric.getStatus() == Status.FAIL) {
					slaveExecutorService.shutdownNow();
					throw new DataXException(
							FrameworkErrorCode.PLUGIN_RUNTIME_ERROR,
							"Plugin run failed .");
				}

                Metric runMetric = MetricManager.getReportMetric(nowMetric,
                        lastMetric, totalSlices);
                frameworkCollector.report(runMetric);
                ErrorRecordLimit.checkLimit(runMetric);

				if (slaveExecutorService.isTerminated() && !hasSlaveException()) {
					LOG.info("Scheduler accomplished all jobs.");
					break;
				}

				lastMetric = nowMetric;
				Thread.sleep(masterReportIntervalInMillSec);
			} while (true);
		} catch (InterruptedException e) {
			LOG.error("InterruptedException caught!", e);
			throw new DataXException(FrameworkErrorCode.INNER_ERROR, e);
		}
	}

	private SlaveContainerRunner newSlaveContainerRunner(
			Configuration configuration) {
		AbstractContainer slaveContainer = ClassUtil.instantiate(configuration
				.getString(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_CLASS),
				AbstractContainer.class, configuration);

		return new SlaveContainerRunner(slaveContainer);
	}

	private boolean hasSlaveException() {
		for (SlaveContainerRunner slaveContainerRunner : slaveContainerRunnerList) {
			if (slaveContainerRunner.getStatus() != Status.SUCCESS) {
				throw new DataXException(
						FrameworkErrorCode.PLUGIN_RUNTIME_ERROR,
						String.format("Slave runs failed."));
			}
		}
		return false;
	}
}
