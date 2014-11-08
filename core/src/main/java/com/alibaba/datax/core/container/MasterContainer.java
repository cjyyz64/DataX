package com.alibaba.datax.core.container;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.MasterPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.core.container.util.ClassLoaderSwapper;
import com.alibaba.datax.core.container.util.LoadUtil;
import com.alibaba.datax.core.scheduler.ErrorRecordLimit;
import com.alibaba.datax.core.scheduler.Scheduler;
import com.alibaba.datax.core.scheduler.standalone.StandAloneScheduler;
import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.statistics.collector.plugin.DefaultMasterPluginCollector;
import com.alibaba.datax.core.statistics.metric.Metric;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.Status;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 * master实例运行在masterContainer容器中，它是所有任务master，负责初始化、拆分、调度、运行、回收、监控和汇报
 * 但它并不做实际的数据同步操作
 */
public class MasterContainer extends AbstractContainer {
	private static final Logger LOG = LoggerFactory
			.getLogger(MasterContainer.class);

	private static final SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	private ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper
			.newCurrentThreadClassLoaderSwapper();

	private long masterId;

	private String readerPluginName;

	private String writerPluginName;

	/**
	 * reader和writer master的实例
	 */
	private Reader.Master readerMaster;

	private Writer.Master writerMaster;

	private long startTimeStamp;

	private long endTimeStamp;

	private long startTransferTimeStamp;

	private long endTransferTimeStamp;

	private int needChannelNumber;

	private ErrorRecordLimit errorLimit;

	public MasterContainer(Configuration configuration) {
		super(configuration);
		super.setContainerCollector(ClassUtil.instantiate(
				configuration
						.getString(CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_CONTAINER_MASTERCLASS),
				AbstractContainerCollector.class, configuration));
		errorLimit = new ErrorRecordLimit(configuration);
	}

	/**
	 * master主要负责的工作全部在start()里面，包括init、prepare、split、scheduler、
	 * post以及destroy和statistics
	 */
	@Override
	public void start() {
		LOG.info("DataX MasterContainer starts master.");

		try {
			this.startTimeStamp = System.currentTimeMillis();

			LOG.debug("master starts to do init ...");
			this.init();
			LOG.debug("master starts to do prepare ...");
			this.prepare();
			LOG.debug("master starts to do split ...");
			this.split();
			LOG.debug("master starts to do schedule ...");
			this.schedule();
			LOG.debug("master starts to do post ...");
			this.post();

			LOG.info("DataX masterId [{}] completed successfully.",
					this.masterId);
		} catch (Throwable e) {
			if (e instanceof OutOfMemoryError) {
				this.destroy();
				System.gc();
			}

			Metric masterMetric = super.getContainerCollector().collect();
			masterMetric.setStatus(Status.FAIL);
			masterMetric.setError(e);
			super.getContainerCollector().report(masterMetric);

			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			this.destroy();
			this.endTimeStamp = System.currentTimeMillis();
			this.logStatistics();
		}
	}

	/**
	 * reader和writer的初始化
	 */
	private void init() {
		this.masterId = this.configuration.getLong(
				CoreConstant.DATAX_CORE_CONTAINER_MASTER_ID, -1);

		if (this.masterId < 0) {
			boolean isStandAloneMode = this.configuration.getString(
					CoreConstant.DATAX_CORE_SCHEDULER_CLASS).equalsIgnoreCase(
					StandAloneScheduler.class.getName());
			// standalone模式下默认masterId=0
			if (isStandAloneMode) {
				LOG.info("Set masterId = 0");
				this.masterId = 0;
				this.configuration.set(
						CoreConstant.DATAX_CORE_CONTAINER_MASTER_ID,
						this.masterId);
			} else {
				throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
						"在[local|distribute]模式下没有设置master id.");
			}
		}

		MasterPluginCollector masterPluginCollector = new DefaultMasterPluginCollector(
				this.getContainerCollector());
		this.readerMaster = this.initReaderMaster(masterPluginCollector);
		this.writerMaster = this.initWriterMaster(masterPluginCollector);
	}

	private void prepare() {
		this.prepareReaderMaster();
		this.prepareWriterMaster();
	}

	/**
	 * 执行reader和writer最细粒度的切分，需要注意的是，writer的切分结果要参照reader的切分结果，
	 * 达到切分后数目相等，才能满足1：1的通道模型，所以这里可以将reader和writer的配置整合到一起，
	 * 然后，为避免顺序给读写端带来长尾影响，将整合的结果shuffler掉
	 */
	private void split() {
		this.adjustChannelNumber();

		if (this.needChannelNumber <= 0) {
			this.needChannelNumber = 1;
		}

		List<Configuration> readerSlicesConfigs = this
				.doReaderSplit(this.needChannelNumber);
		int slicesNumber = readerSlicesConfigs.size();
		List<Configuration> writerSlicesConfigs = this
				.doWriterSplit(slicesNumber);

		/**
		 * 输入是reader和writer的parameter list，输出是content下面元素的list
		 */
		List<Configuration> contentConfig = mergeReaderAndWriterSlicesConfigs(
				readerSlicesConfigs, writerSlicesConfigs);

		Collections.shuffle(contentConfig,
				new Random(System.currentTimeMillis()));

		this.configuration.set(CoreConstant.DATAX_JOB_CONTENT, contentConfig);
	}

	private void adjustChannelNumber() {
		boolean isByteLimit = (this.configuration.getInt(
				CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 0) > 0);
		if (isByteLimit) {

			long globalLimitedSpeed = this.configuration
					.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE,
							10 * 1024 * 1024);

			// 在byte流控情况下，单个Channel流量最大值必须设置，否则报错！
			this.configuration.getNecessaryValue(
					CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE,
					FrameworkErrorCode.CONFIG_ERROR);
			long channelLimitedSpeed = this.configuration
					.getInt(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE);

			this.needChannelNumber = (int) (globalLimitedSpeed / channelLimitedSpeed);

			LOG.info("Job set Max-Speed-Byte to " + globalLimitedSpeed
					+ " bytes .");

			return;
		}

		boolean isChannelLimit = (this.configuration.getInt(
				CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL, 0) > 0);
		if (isChannelLimit) {
			this.needChannelNumber = this.configuration
					.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL);

			LOG.info("Job set Max-Speed-Channel to " + this.needChannelNumber
					+ " channels .");

			return;
		}

		throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
				"Job运行速度必须设置");
	}

	/**
	 * schedule首先完成的工作是把上一步reader和writer split的结果整合到具体slaveContainer中,
	 * 同时不同的执行模式调用不同的调度策略，将所有任务调度起来
	 */
	private void schedule() {
		/**
		 * 这里的全局speed和每个channel的速度设置为B/s
		 */
		int channelsPerSlaveContainer = this.configuration.getInt(
				CoreConstant.DATAX_CORE_CONTAINER_SLAVE_CHANNEL, 5);
		int sliceNumber = this.configuration.getList(
				CoreConstant.DATAX_JOB_CONTENT).size();

		this.needChannelNumber = Math.min(this.needChannelNumber, sliceNumber);

		/**
		 * 通过获取配置信息得到每个slaveContainer需要运行哪些slices任务
		 */
		int averSlicesPerChannel = sliceNumber / this.needChannelNumber;
		List<Configuration> slaveContainerConfigs = distributeSlicesToSlaveContainer(
				averSlicesPerChannel, this.needChannelNumber,
				channelsPerSlaveContainer);

		LOG.info("Scheduler starts [{}] slaves .", slaveContainerConfigs.size());

		String schedulerClassName = this.configuration
				.getString(CoreConstant.DATAX_CORE_SCHEDULER_CLASS);
		LOG.info("Scheduler [{}] activated.", schedulerClassName);

		try {
			Scheduler scheduler = ClassUtil.instantiate(schedulerClassName,
					Scheduler.class);

			this.startTransferTimeStamp = System.currentTimeMillis();
			scheduler.schedule(slaveContainerConfigs,
					super.getContainerCollector());
			this.endTransferTimeStamp = System.currentTimeMillis();
		} catch (Exception e) {
			LOG.error("运行scheduler[[{}]]出错", schedulerClassName);
			this.endTransferTimeStamp = System.currentTimeMillis();
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		}

		/**
		 * 检查任务执行情况
		 */
		this.checkLimit();
	}

	private void post() {
		this.postWriterMaster();
		this.postReaderMaster();
	}

	private void destroy() {
		if (this.writerMaster != null) {
			this.writerMaster.destroy();
			this.writerMaster = null;
		}
		if (this.readerMaster != null) {
			this.readerMaster.destroy();
			this.readerMaster = null;
		}
	}

	private void logStatistics() {
		long totalCosts = (this.endTimeStamp - this.startTimeStamp) / 1000;
		long transferCosts = (this.endTransferTimeStamp - this.startTransferTimeStamp) / 1000;
		if (0L == transferCosts) {
			transferCosts = 1L;
		}

		Metric masterMetric = super.getContainerCollector().collect();
		LOG.info(String.format(
				"\n" + "%-26s: %-18s\n" + "%-26s: %-18s\n" + "%-26s: %19s\n"
						+ "%-26s: %19s\n" + "%-26s: %19s\n" + "%-26s: %19s\n"
						+ "%-26s: %19s\n",
				"DataX 启动时刻:",
				dateFormat.format(startTimeStamp),

				"DataX 结束时刻:",
				dateFormat.format(endTimeStamp),

				"本任务耗时:",
				String.valueOf(totalCosts) + "s",
				"平均流量",
				StrUtil.stringify(masterMetric.getReadSucceedBytes()
						/ transferCosts)
						+ "/s",
				"记录写入速度",
				String.valueOf(masterMetric.getReadSucceedRecords()
						/ transferCosts)
						+ "rec/s", "读出行总数",
				String.valueOf(masterMetric.getTotalReadRecords()),
				"脏行总数",
				String.valueOf(masterMetric.getErrorRecords())));
	}

	/**
	 * reader master的初始化，返回Reader.Master
	 * 
	 * @return
	 */
	private Reader.Master initReaderMaster(
			MasterPluginCollector masterPluginCollector) {
		this.readerPluginName = this.configuration
				.getString(CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
		classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
				PluginType.READER, this.readerPluginName));

		Reader.Master readerMaster = (Reader.Master) LoadUtil.loadMasterPlugin(
				PluginType.READER, this.readerPluginName);

		// 设置reader的jobConfig
		readerMaster
				.setPluginJobConf(this.configuration
						.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));
		readerMaster.setMasterPluginCollector(masterPluginCollector);
		readerMaster.init();

		classLoaderSwapper.restoreCurrentThreadClassLoader();
		return readerMaster;
	}

	/**
	 * writer master的初始化，返回Writer.Master
	 * 
	 * @return
	 */
	private Writer.Master initWriterMaster(
			MasterPluginCollector masterPluginCollector) {
		this.writerPluginName = this.configuration
				.getString(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
		classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
				PluginType.WRITER, this.writerPluginName));

		Writer.Master writerMaster = (Writer.Master) LoadUtil.loadMasterPlugin(
				PluginType.WRITER, this.writerPluginName);

		// 设置writer的jobConfig
		writerMaster
				.setPluginJobConf(this.configuration
						.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));
		writerMaster.setMasterPluginCollector(masterPluginCollector);
		writerMaster.init();
		classLoaderSwapper.restoreCurrentThreadClassLoader();

		return writerMaster;
	}

	private void prepareReaderMaster() {
		classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
				PluginType.READER, this.readerPluginName));
		LOG.info(String.format("DataX Reader.Master [%s] do prepare work .",
				this.readerPluginName));
		this.readerMaster.prepare();
		classLoaderSwapper.restoreCurrentThreadClassLoader();
	}

	private void prepareWriterMaster() {
		classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
				PluginType.WRITER, this.writerPluginName));
		LOG.info(String.format("DataX Writer.Master [%s] do prepare work .",
				this.writerPluginName));
		this.writerMaster.prepare();
		classLoaderSwapper.restoreCurrentThreadClassLoader();
	}

	// TODO: 如果源头就是空数据
	private List<Configuration> doReaderSplit(int adviceNumber) {
		classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
				PluginType.READER, this.readerPluginName));
		List<Configuration> readerSlicesConfigs = this.readerMaster
				.split(adviceNumber);
		if (readerSlicesConfigs == null || readerSlicesConfigs.size() <= 0) {
			throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
					"reader切分的slice数目不能小于等于0");
		}
		LOG.info("DataX Reader.Master [{}] splits to [{}] slices.",
				this.readerPluginName, readerSlicesConfigs.size());
		classLoaderSwapper.restoreCurrentThreadClassLoader();
		return readerSlicesConfigs;
	}

	private List<Configuration> doWriterSplit(int readerSlicesNumber) {
		classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
				PluginType.WRITER, this.writerPluginName));

		List<Configuration> writerSlicesConfigs = this.writerMaster
				.split(readerSlicesNumber);
		if (writerSlicesConfigs == null || writerSlicesConfigs.size() <= 0) {
			throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
					"writer切分的slice不能小于等于0");
		}
		LOG.info("DataX Writer [{}] splits to [{}] slices.",
				this.writerPluginName, writerSlicesConfigs.size());
		classLoaderSwapper.restoreCurrentThreadClassLoader();

		return writerSlicesConfigs;
	}

	/**
	 * 按顺序整合reader和writer的配置，这里的顺序不能乱！ 输入是reader、writer级别的配置，输出是一个完整slice的配置
	 * 
	 * @param readerSlicesConfigs
	 * @param writerSlicesConfigs
	 * @return
	 */
	private List<Configuration> mergeReaderAndWriterSlicesConfigs(
			List<Configuration> readerSlicesConfigs,
			List<Configuration> writerSlicesConfigs) {
		if (readerSlicesConfigs.size() != writerSlicesConfigs.size()) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
					String.format(
							"reader切分的slice数目[%d]不等于writer切分的slice数目[%d].",
							readerSlicesConfigs.size(),
							writerSlicesConfigs.size()));
		}

		List<Configuration> contentConfigs = new ArrayList<Configuration>();
		for (int i = 0; i < readerSlicesConfigs.size(); i++) {
			Configuration sliceConfig = Configuration.newDefault();
			sliceConfig
					.set(CoreConstant.JOB_READER_NAME, this.readerPluginName);
			sliceConfig.set(CoreConstant.JOB_READER_PARAMETER,
					readerSlicesConfigs.get(i));
			sliceConfig
					.set(CoreConstant.JOB_WRITER_NAME, this.writerPluginName);
			sliceConfig.set(CoreConstant.JOB_WRITER_PARAMETER,
					writerSlicesConfigs.get(i));
			sliceConfig.set(CoreConstant.JOB_SLICEID, i);
			contentConfigs.add(sliceConfig);
		}

		return contentConfigs;
	}

	/**
	 * 这里比较复杂，分两步整合 1. slices到channel 2. channel到slaveContainer
	 * 合起来考虑，其实就是把slices整合到slaveContainer中，需要满足计算出的channel数，同时不能多起channel
	 * <p/>
	 * example:
	 * <p/>
	 * 前提条件： 切分后是1024个分表，假设用户要求总速率是1000M/s，每个channel的速率的3M/s，
	 * 每个slaveContainer负责运行7个channel
	 * <p/>
	 * 计算： 总channel数为：1000M/s / 3M/s =
	 * 333个，为平均分配，计算可知有308个每个channel有3个slices，而有25个每个channel有4个slices，
	 * 需要的slaveContainer数为：333 / 7 =
	 * 47...4，也就是需要48个slaveContainer，47个是每个负责7个channel，有4个负责1个channel
	 * <p/>
	 * 处理：我们先将这负责4个channel的slaveContainer处理掉，逻辑是：
	 * 先按平均为3个slices找4个channel，设置subJobId为0，
	 * 接下来就像发牌一样轮询分配slice到剩下的包含平均channel数的slice中
	 * 
	 * @param averSlicesPerChannel
	 * @param channelNumber
	 * @param channelsPerSlaveContainer
	 * @return 每个slaveContainer独立的全部配置
	 */
	@SuppressWarnings("serial")
	private List<Configuration> distributeSlicesToSlaveContainer(
			int averSlicesPerChannel, int channelNumber,
			int channelsPerSlaveContainer) {
		Validate.isTrue(averSlicesPerChannel > 0 && channelNumber > 0
				&& channelsPerSlaveContainer > 0,
				"每个channel的平均slice数[averSlicesPerChannel]，channel数目[channelNumber]，每个slave的平均channel数[channelsPerSlaveContainer]都应该为正数");
		List<Configuration> slicesConfigs = this.configuration
				.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
		int slavesNumber = channelNumber / channelsPerSlaveContainer;
		int leftChannelNumber = channelNumber % channelsPerSlaveContainer;
		if (leftChannelNumber > 0) {
			slavesNumber += 1;
		}

		/**
		 * 如果只有一个slave，直接打标返回
		 */
		if (slavesNumber == 1) {
			final Configuration slaveConfig = this.configuration.clone();
			/**
			 * configure的clone不能clone出
			 */
			slaveConfig.set(CoreConstant.DATAX_JOB_CONTENT, this.configuration
					.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT));
			slaveConfig.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_CHANNEL,
					channelNumber);
			slaveConfig.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID, 0);
			return new ArrayList<Configuration>() {
				{
					add(slaveConfig);
				}
			};
		}
		List<Configuration> slaveConfigs = new ArrayList<Configuration>();
		/**
		 * 将每个slaveConfig中content的配置清空
		 */
		for (int i = 0; i < slavesNumber; i++) {
			Configuration slaveConfig = this.configuration.clone();
			List<Configuration> slaveJobContent = slaveConfig
					.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
			slaveJobContent.clear();
			slaveConfig.set(CoreConstant.DATAX_JOB_CONTENT, slaveJobContent);

			slaveConfigs.add(slaveConfig);
		}

		int slicesConfigIndex = 0;
		int channelIndex = 0;
		int slavesConfigIndex = 0;

		/**
		 * 先处理掉slaveContainer包含channel数不是平均值的slave
		 */
		if (leftChannelNumber > 0) {
			Configuration slaveConfig = slaveConfigs.get(slavesConfigIndex);
			for (; channelIndex < leftChannelNumber; channelIndex++) {
				for (int i = 0; i < averSlicesPerChannel; i++) {
					List<Configuration> slaveJobConfigs = slaveConfig
							.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
					slaveJobConfigs.add(slicesConfigs.get(slicesConfigIndex++));
					slaveConfig.set(CoreConstant.DATAX_JOB_CONTENT,
							slaveJobConfigs);
				}
			}

			slaveConfig.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_CHANNEL,
					leftChannelNumber);
			slaveConfig.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID,
					slavesConfigIndex++);
		}

		/**
		 * 下面需要轮询分配，并打上channel数和slaveId标记
		 */
		int equalDivisionStartIndex = slavesConfigIndex;
		for (; slicesConfigIndex < slicesConfigs.size()
				&& equalDivisionStartIndex < slaveConfigs.size();) {
			for (slavesConfigIndex = equalDivisionStartIndex; slavesConfigIndex < slaveConfigs
					.size() && slicesConfigIndex < slicesConfigs.size(); slavesConfigIndex++) {
				Configuration slaveConfig = slaveConfigs.get(slavesConfigIndex);
				List<Configuration> slaveJobConfigs = slaveConfig
						.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
				slaveJobConfigs.add(slicesConfigs.get(slicesConfigIndex++));
				slaveConfig
						.set(CoreConstant.DATAX_JOB_CONTENT, slaveJobConfigs);
			}
		}

		for (slavesConfigIndex = equalDivisionStartIndex; slavesConfigIndex < slaveConfigs
				.size();) {
			Configuration slaveConfig = slaveConfigs.get(slavesConfigIndex);
			slaveConfig.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_CHANNEL,
					channelsPerSlaveContainer);
			slaveConfig.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID,
					slavesConfigIndex++);
		}

		return slaveConfigs;
	}

	private void postReaderMaster() {
		classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
				PluginType.READER, this.readerPluginName));
		LOG.info("DataX Reader.Master [{}] do post work.",
				this.readerPluginName);
		this.readerMaster.post();
		classLoaderSwapper.restoreCurrentThreadClassLoader();
	}

	private void postWriterMaster() {
		classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
				PluginType.WRITER, this.writerPluginName));
		LOG.info("DataX Writer.Master [{}] do post work.",
				this.writerPluginName);
		this.writerMaster.post();
		classLoaderSwapper.restoreCurrentThreadClassLoader();
	}

	/**
	 * 检查最终结果是否超出阈值，如果阈值设定小于1，则表示百分数阈值，大于1表示条数阈值。
	 * 
	 * @param
	 */
	private void checkLimit() {
		Metric masterMetric = super.getContainerCollector().collect();
		errorLimit.checkRecordLimit(masterMetric);
		errorLimit.checkPercentageLimit(masterMetric);
	}
}
