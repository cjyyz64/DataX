package com.alibaba.datax.core.container;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.core.util.*;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.runner.AbstractRunner;
import com.alibaba.datax.core.container.runner.ReaderRunner;
import com.alibaba.datax.core.container.runner.RunnerManager;
import com.alibaba.datax.core.container.runner.WriterRunner;
import com.alibaba.datax.core.container.util.LoadUtil;
import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.collector.plugin.slave.AbstractSlavePluginCollector;
import com.alibaba.datax.core.statistics.metric.Metric;
import com.alibaba.datax.core.statistics.metric.MetricManager;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordExchanger;
import com.alibaba.fastjson.JSON;

/**
 * Created by jingxing on 14-8-24.
 */
public class SlaveContainer extends AbstractContainer {
	private static final Logger LOG = LoggerFactory
			.getLogger(SlaveContainer.class);

	/**
	 * 当前slaveContainer所属masterContainerId
	 */
	private long masterId;

	/**
	 * 当前slaveContainerId
	 */
	private int slaveId;

	public SlaveContainer(Configuration configuration) {
		super(configuration);
		super.setContainerCollector(ClassUtil.instantiate(
				configuration
						.getString(CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_CONTAINER_SLAVECLASS),
				AbstractContainerCollector.class, configuration));
		this.masterId = this.configuration
				.getLong(CoreConstant.DATAX_CORE_CONTAINER_MASTER_ID);
		this.slaveId = this.configuration
				.getInt(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID);
	}

	public long getMasterId() {
		return masterId;
	}

	public int getSlaveId() {
		return slaveId;
	}

	@Override
	public void start() {
		try {
			List<Configuration> jobsConfigs = this.configuration
					.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);

			LOG.debug("slave[{}] slices configs[{}]", this.slaveId,
					JSON.toJSONString(jobsConfigs));

			/**
			 * 状态check时间间隔，较短，可以把任务及时分发到对应channel中
			 */
			int sleepIntervalInMillSec = this.configuration.getInt(
					CoreConstant.DATAX_CORE_CONTAINER_SLAVE_SLEEPINTERVAL, 100);
			/**
			 * 状态汇报时间间隔，稍长，避免大量汇报
			 */
			long reportIntervalInMillSec = this.configuration.getLong(
					CoreConstant.DATAX_CORE_CONTAINER_SLAVE_REPORTINTERVAL,
					5000);

			int channelNumber = this.configuration
					.getInt(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_CHANNEL);

			List<WorkUnit> workUnits = new ArrayList<WorkUnit>();
			for (int i = 0; i < channelNumber; i++) {
				/**
				 * 这里要设置channelId，所以clone了全局配置
				 */
				Configuration setChannelIdConfig = this.configuration.clone();
				setChannelIdConfig.set(
						CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_ID, i);
				MetricManager.registerMetric(this.slaveId, i);
				String channelClazz = this.configuration
						.getString(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CLASS);

				workUnits.add(new WorkUnit(ClassUtil.instantiate(channelClazz,
						Channel.class, setChannelIdConfig)));
			}

			LOG.info(String.format(
					"SlaveId=[%d] start [%d] channels for [%d] slices.",
					this.slaveId, channelNumber, jobsConfigs.size()));

			long lastReportTimeStamp = 0;
			int jobIndex = 0;
			while (true) {
				List<AbstractRunner> runnerList = RunnerManager
						.getRunners(this.slaveId);
				if (null != runnerList) {
					for (AbstractRunner runner : runnerList) {
						if (runner.getRunnerStatus().isFail()) {
							Metric nowMetric = super.getContainerCollector()
									.collect();
                            Throwable throwable = nowMetric.getError();
                            if(throwable instanceof OutOfMemoryError) {
                                // 释放plugin空间，且可以将OOM状态汇报上去
                                runner.destroy();
                            }
							nowMetric.setError(throwable);
							nowMetric.setStatus(Status.FAIL);
							super.getContainerCollector().report(nowMetric);

							throw DataXException.asDataXException(
									FrameworkErrorCode.PLUGIN_RUNTIME_ERROR,
                                    throwable);
						}
					}
				}

				for (WorkUnit unit : workUnits) {
					if (unit.isTaskDone()) {
						LOG.debug("Task completed, prepare to switch to new Task.");

						if (jobIndex >= jobsConfigs.size()) {
							break;
						}

						unit.assignTask(jobsConfigs.get(jobIndex++));
					}
				}

				boolean isWorkAllDone = true;
				for (WorkUnit taskUnit : workUnits) {
					if (!taskUnit.isTaskDone()) {
						isWorkAllDone = false;
						break;
					}
				}

				if (jobIndex >= jobsConfigs.size() && isWorkAllDone
						&& isAllRunnerSuccess()) {
					LOG.info("Slave[{}] complete job.", this.slaveId);
					break;
				}

				// 如果当前时间已经超出汇报时间的interval，那么我们需要马上汇报
				long now = System.currentTimeMillis();
				if (now - lastReportTimeStamp > reportIntervalInMillSec) {
                    Metric nowMetric = super.getContainerCollector().collect();
                    super.getContainerCollector().report(nowMetric);
					lastReportTimeStamp = now;
				}

				Thread.sleep(sleepIntervalInMillSec);
			}

			Metric nowMetric = super.getContainerCollector().collect();
			nowMetric.setStatus(Status.SUCCESS);
			super.getContainerCollector().report(nowMetric);

		} catch (Throwable e) {
			Metric nowMetric = super.getContainerCollector().collect();

			nowMetric.setError(nowMetric.getError() != null ? nowMetric
					.getError() : e);
			nowMetric.setStatus(Status.FAIL);
			super.getContainerCollector().report(nowMetric);

			throw DataXException.asDataXException(
					FrameworkErrorCode.INNER_ERROR, e);
		}
	}

	private boolean isAllRunnerSuccess() {
		boolean isAllSuccess = true;
		if (null == RunnerManager.getRunners(this.slaveId)) {
			isAllSuccess = false;
		} else {
			for (AbstractRunner runner : RunnerManager.getRunners(this.slaveId)) {
				if (!runner.getRunnerStatus().isSuccess()) {
					isAllSuccess = false;
					break;
				}
			}
		}
		return isAllSuccess;
	}

	/**
	 * WorkUnit持有一个channel，多个slices可以在同一个WorkUnit中运行 其中包括1：1的reader和writer
	 */
	class WorkUnit {
		private Channel channel;

		private Thread readerThread;

		private Thread writerThread;

		/**
		 * masterId 和 slaveId 在外部类已有 channelId属于一个workUnit sliceId在每次提交任务中
		 */
		private int channelId;

		public WorkUnit(Channel channel) {
			this.channel = channel;
			this.channelId = this.channel.getChannelId();
		}

		public void assignTask(Configuration jobConf) {
			Validate.isTrue(
					null != jobConf.getConfiguration(CoreConstant.JOB_READER)
							&& null != jobConf
									.getConfiguration(CoreConstant.JOB_WRITER),
					"reader/writer PluginParam cannot be empty!");

			if (!isTaskDone()) {
				throw new DataXException(FrameworkErrorCode.INNER_ERROR,
						"Work not completed yet!");
			}

			int sliceId = jobConf.getInt(CoreConstant.JOB_SLICEID);
			String slaveCollectorClass = configuration
					.getString(CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_SLAVECLASS);

			// 首先启动writer，那么如果writer挂了，reader就不必启动了，从而避免reader先启动把数据写进channel，
			// 而下一个任务如果还继续用该channel，导致数据污染
			WriterRunner writerRunner = (WriterRunner) LoadUtil
					.loadPluginRunner(PluginType.WRITER,
							jobConf.getString(CoreConstant.JOB_WRITER_NAME),
							slaveId);
			writerRunner.setJobConf(jobConf
					.getConfiguration(CoreConstant.JOB_WRITER_PARAMETER));
			writerRunner.setRecordReceiver(new BufferedRecordExchanger(
					this.channel));

			writerRunner.setChannelId(this.channel.getChannelId());
			writerRunner.setSlaveId(this.channel.getSlaveId());

			/**
			 * 设置slavePlugin的collector，用来处理脏数据和master/slave通信
			 */
			writerRunner.setSlavePluginCollector(ClassUtil.instantiate(
					slaveCollectorClass, AbstractSlavePluginCollector.class,
					configuration, this.channel.getChannelMetric(),
					PluginType.WRITER));

			this.writerThread = new Thread(writerRunner,
					String.format("%d-%d-%d-%d-writer", masterId, slaveId,
							channelId, sliceId));
			this.writerThread.setContextClassLoader(LoadUtil.getJarLoader(
					PluginType.WRITER,
					jobConf.getString(CoreConstant.JOB_WRITER_NAME)));
			this.writerThread.start();

			// bug: 如果启动伊始，writer即挂，这里需要判断
			while (!this.writerThread.isAlive()) {
				if (writerRunner.getRunnerStatus().isFail()) {
					throw new DataXException(FrameworkErrorCode.INNER_ERROR,
							MetricManager.getChannelMetric(
									this.channel.getSlaveId(),
									this.channel.getChannelId()).getError());
				} else {
					break;
				}
			}

			/**
			 * 生成readerRunner同时注册到RunnerManager中
			 */
			ReaderRunner readerRunner = (ReaderRunner) LoadUtil
					.loadPluginRunner(PluginType.READER,
							jobConf.getString(CoreConstant.JOB_READER_NAME),
							slaveId);

			readerRunner.setJobConf(jobConf
					.getConfiguration(CoreConstant.JOB_READER_PARAMETER));
			readerRunner.setRecordSender(new BufferedRecordExchanger(
					this.channel));

			readerRunner.setChannelId(this.channel.getChannelId());
			readerRunner.setSlaveId(this.channel.getSlaveId());

			/**
			 * 设置slavePlugin的collector，用来处理脏数据和master/slave通信
			 */
			readerRunner.setSlavePluginCollector(ClassUtil.instantiate(
					slaveCollectorClass, AbstractSlavePluginCollector.class,
					configuration, this.channel.getChannelMetric(),
					PluginType.READER));
			/**
			 * 通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
			 */
			this.readerThread = new Thread(readerRunner,
					String.format("%d-%d-%d-%d-reader", masterId, slaveId,
							channelId, sliceId));
			this.readerThread.setContextClassLoader(LoadUtil.getJarLoader(
					PluginType.READER,
					jobConf.getString(CoreConstant.JOB_READER_NAME)));
			this.readerThread.start();

			// 我们担心在isTaskDone函数检查Reader/Writer 是否存活时候存在时序问题，因此这里先优先保证启动成功
			while (!this.readerThread.isAlive()) {
				// 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
				if (readerRunner.getRunnerStatus().isFail()) {
					throw new DataXException(FrameworkErrorCode.INNER_ERROR,
							MetricManager.getChannelMetric(
									this.channel.getSlaveId(),
									this.channel.getChannelId()).getError());
				} else {
					break;
				}
			}
		}

		// 检查reader、writer线程是否完成工作
		private boolean isTaskDone() {
			// 如果没有reader/writer 那就是才初始化，可以理解为完成了工作
			if (null == readerThread || null == writerThread) {
				return true;
			}

			// 如果reader 或 writer没有完成工作，那么直接返回工作没有完成
			if (readerThread.isAlive() || writerThread.isAlive()) {
				return false;
			}

			/**
			 * 如果reader或writer异常退出了，但channel中的数据并没有消费完，这时还不能算完， 需要抛出异常，等待上面处理
			 */
			if (!this.channel.isEmpty()) {
				return false;
			}

			return true;
		}
	}
}
