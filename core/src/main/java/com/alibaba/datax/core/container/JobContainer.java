package com.alibaba.datax.core.container;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.JobPluginCollector;
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
import com.alibaba.datax.core.statistics.collector.plugin.DefaultJobPluginCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.State;
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
 * job实例运行在jobContainer容器中，它是所有任务的master，负责初始化、拆分、调度、运行、回收、监控和汇报
 * 但它并不做实际的数据同步操作
 */
public class JobContainer extends AbstractContainer {
    private static final Logger LOG = LoggerFactory
            .getLogger(JobContainer.class);

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");

    private ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper
            .newCurrentThreadClassLoaderSwapper();

    private long jobId;

    private String readerPluginName;

    private String writerPluginName;

    /**
     * reader和writer jobContainer的实例
     */
    private Reader.Job jobReader;

    private Writer.Job jobWriter;

    private long startTimeStamp;

    private long endTimeStamp;

    private long startTransferTimeStamp;

    private long endTransferTimeStamp;

    private int needChannelNumber;

    private ErrorRecordLimit errorLimit;

    public JobContainer(Configuration configuration) {
        super(configuration);

        //TODO 可能需要考虑采用 set方式在使用前进行初始化
        super.setContainerCollector(ClassUtil.instantiate(
                configuration.getString(
                        CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_CONTAINER_JOBCLASS),
                AbstractContainerCollector.class, configuration));
        errorLimit = new ErrorRecordLimit(configuration);
    }

    /**
     * jobContainer主要负责的工作全部在start()里面，包括init、prepare、split、scheduler、
     * post以及destroy和statistics
     */
    @Override
    public void start() {
        LOG.info("DataX jobContainer starts job.");

        try {
            this.startTimeStamp = System.currentTimeMillis();

            LOG.debug("jobContainer starts to do init ...");
            this.init();
            LOG.debug("jobContainer starts to do prepare ...");
            this.prepare();
            LOG.debug("jobContainer starts to do split ...");
            this.split();
            LOG.debug("jobContainer starts to do schedule ...");
            this.schedule();
            LOG.debug("jobContainer starts to do post ...");
            this.post();

            LOG.info("DataX jobId [{}] completed successfully.",
                    this.jobId);
        } catch (Throwable e) {
            if (e instanceof OutOfMemoryError) {
                this.destroy();
                System.gc();
            }

            Communication communication =
                    super.getContainerCollector().collect();
            communication.setState(State.FAIL);
            communication.setThrowable(e);
            super.getContainerCollector().report(communication);

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
        this.jobId = this.configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, -1);

        if (this.jobId < 0) {
            boolean isStandAloneMode = this.configuration.getString(
                    CoreConstant.DATAX_CORE_SCHEDULER_CLASS).equalsIgnoreCase(
                    StandAloneScheduler.class.getName());
            // standalone模式下默认jobId=0
            if (isStandAloneMode) {
                LOG.info("Set jobId = 0");
                this.jobId = 0;
                this.configuration.set(
                        CoreConstant.DATAX_CORE_CONTAINER_JOB_ID,
                        this.jobId);
            } else {
                throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
                        "在[local|distribute]模式下没有设置jobId.");
            }
        }

        Thread.currentThread().setName(
                String.format("job-%d", this.jobId));

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                this.getContainerCollector());
        this.jobReader = this.initJobReader(jobPluginCollector);
        this.jobWriter = this.initJobWriter(jobPluginCollector);
    }

    private void prepare() {
        this.prepareJobReader();
        this.prepareJobWriter();
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

        List<Configuration> readerTaskConfigs = this
                .doReaderSplit(this.needChannelNumber);
        int taskNumber = readerTaskConfigs.size();
        List<Configuration> writerTaskConfigs = this
                .doWriterSplit(taskNumber);

        /**
         * 输入是reader和writer的parameter list，输出是content下面元素的list
         */
        List<Configuration> contentConfig = mergeReaderAndWriterTaskConfigs(
                readerTaskConfigs, writerTaskConfigs);

        Collections.shuffle(contentConfig,
                new Random(System.currentTimeMillis()));

        this.configuration.set(CoreConstant.DATAX_JOB_CONTENT, contentConfig);
    }

    private void adjustChannelNumber() {
        int needChannelNumberByByte = Integer.MAX_VALUE;
        int needChannelNumberByRecord = Integer.MAX_VALUE;

        boolean isByteLimit = (this.configuration.getInt(
                CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 0) > 0);
        if (isByteLimit) {
            long globalLimitedByteSpeed = this.configuration.getInt(
                    CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 10 * 1024 * 1024);

            // 在byte流控情况下，单个Channel流量最大值必须设置，否则报错！
            Long channelLimitedByteSpeed = this.configuration
                    .getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE);
            if (channelLimitedByteSpeed == null || channelLimitedByteSpeed <= 0) {
                DataXException.asDataXException(
                        FrameworkErrorCode.CONFIG_ERROR,
                        "在有总bps限速条件下，单个channel的bps值不能为空，也不能为非正数");
            }

            needChannelNumberByByte =
                    (int) (globalLimitedByteSpeed / channelLimitedByteSpeed);
            needChannelNumberByByte =
                    needChannelNumberByByte > 0 ? needChannelNumberByByte : 1;
            LOG.info("Job set Max-Byte-Speed to " + globalLimitedByteSpeed + " bytes.");
        }

        boolean isRecordLimit = (this.configuration.getInt(
                CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 0)) > 0;
        if (isRecordLimit) {
            long globalLimitedRecordSpeed = this.configuration.getInt(
                    CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 100000);

            Long channelLimitedRecordSpeed = this.configuration.getLong(
                    CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_RECORD);
            if (channelLimitedRecordSpeed == null || channelLimitedRecordSpeed <= 0) {
                DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                        "在有总tps限速条件下，单个channel的tps值不能为空，也不能为非正数");
            }

            needChannelNumberByRecord =
                    (int) (globalLimitedRecordSpeed / channelLimitedRecordSpeed);
            needChannelNumberByRecord =
                    needChannelNumberByRecord > 0 ? needChannelNumberByRecord : 1;
            LOG.info("Job set Max-Record-Speed to " + globalLimitedRecordSpeed + " records.");
        }

        // 取较小值
        this.needChannelNumber = needChannelNumberByByte < needChannelNumberByRecord ?
                needChannelNumberByByte : needChannelNumberByRecord;

        // 如果从byte或record上设置了needChannelNumber则退出
        if (this.needChannelNumber < Integer.MAX_VALUE) {
            return;
        }

        boolean isChannelLimit = (this.configuration.getInt(
                CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL, 0) > 0);
        if (isChannelLimit) {
            this.needChannelNumber = this.configuration.getInt(
                    CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL);

            LOG.info("Job set Channel-Number to " + this.needChannelNumber
                    + " channels.");

            return;
        }

        throw DataXException.asDataXException(
                FrameworkErrorCode.CONFIG_ERROR,
                "Job运行速度必须设置");
    }

    /**
     * schedule首先完成的工作是把上一步reader和writer split的结果整合到具体taskGroupContainer中,
     * 同时不同的执行模式调用不同的调度策略，将所有任务调度起来
     */
    private void schedule() {
        /**
         * 这里的全局speed和每个channel的速度设置为B/s
         */
        int channelsPerTaskGroup = this.configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, 5);
        int taskNumber = this.configuration.getList(
                CoreConstant.DATAX_JOB_CONTENT).size();

        this.needChannelNumber = Math.min(this.needChannelNumber, taskNumber);

        /**
         * 通过获取配置信息得到每个taskGroup需要运行哪些tasks任务
         */
        int averTasksPerChannel = taskNumber / this.needChannelNumber;
        List<Configuration> taskGroupConfigs = distributeTasksToTaskGroup(
                averTasksPerChannel, this.needChannelNumber,
                channelsPerTaskGroup);

        LOG.info("Scheduler starts [{}] taskGroups.", taskGroupConfigs.size());


        // 判断是否为分布式模式运行
        if (taskGroupConfigs != null && taskGroupConfigs.size() > 1) {
            /**
             * TODO  如果 用户强行配置了只使用单机模式运行，则需要对对应的configuration 的配置项改动
             * 否则按照 完全分布式模式运行
             */
            if("local".equalsIgnoreCase(this.configuration.getString("TODO"))){
                this.configuration.set(CoreConstant.DATAX_CORE_SCHEDULER_CLASS,
                        "com.alibaba.datax.core.scheduler.local.LocalScheduler");

                //TODO 还有其他的配置需要刷新
                /**
                 * "jobClass": "com.alibaba.datax.core.statistics.collector.container.standalone.JobContainerCollector",
                 "taskGroupClass": "com.alibaba.datax.core.statistics.collector.container.standalone.TaskGroupContainerCollector"
                 */
            }else{
                // 分布式运行模式
                this.configuration.set(CoreConstant.DATAX_CORE_SCHEDULER_CLASS,
                        "com.alibaba.datax.core.scheduler.distribute.DistributeScheduler");
                //TODO 还有其他的配置需要刷新
            }

        }


        String schedulerClassName = this.configuration.getString(
                CoreConstant.DATAX_CORE_SCHEDULER_CLASS);
        LOG.info("Scheduler [{}] activated.", schedulerClassName);

        try {
            Scheduler scheduler = ClassUtil.instantiate(
                    schedulerClassName, Scheduler.class);

            this.startTransferTimeStamp = System.currentTimeMillis();
            scheduler.schedule(taskGroupConfigs,
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
        this.postJobWriter();
        this.postJobReader();
    }

    private void destroy() {
        if (this.jobWriter != null) {
            this.jobWriter.destroy();
            this.jobWriter = null;
        }
        if (this.jobReader != null) {
            this.jobReader.destroy();
            this.jobReader = null;
        }
    }

    private void logStatistics() {
        long totalCosts = (this.endTimeStamp - this.startTimeStamp) / 1000;
        long transferCosts = (this.endTransferTimeStamp - this.startTransferTimeStamp) / 1000;
        if (0L == transferCosts) {
            transferCosts = 1L;
        }

        Communication communication = super.getContainerCollector().collect();
        LOG.info(String.format(
                "\n" + "%-26s: %-18s\n" + "%-26s: %-18s\n" + "%-26s: %19s\n"
                        + "%-26s: %19s\n" + "%-26s: %19s\n" + "%-26s: %19s\n"
                        + "%-26s: %19s\n",
                "任务启动时刻",
                dateFormat.format(startTimeStamp),

                "任务结束时刻",
                dateFormat.format(endTimeStamp),

                "任务总计耗时",
                String.valueOf(totalCosts) + "s",
                "任务平均流量",
                StrUtil.stringify(communication.getLongCounter(CommunicationManager.READ_SUCCEED_BYTES)
                        / transferCosts)
                        + "/s",
                "记录写入速度",
                String.valueOf(communication.getLongCounter(CommunicationManager.READ_SUCCEED_RECORDS)
                        / transferCosts)
                        + "rec/s", "读出记录总数",
                String.valueOf(CommunicationManager.getTotalReadRecords(communication)),
                "读写失败总数",
                String.valueOf(CommunicationManager.getTotalErrorRecords(communication))));
    }

    /**
     * reader job的初始化，返回Reader.Job
     *
     * @return
     */
    private Reader.Job initJobReader(
            JobPluginCollector jobPluginCollector) {
        this.readerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));

        Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(
                PluginType.READER, this.readerPluginName);

        // 设置reader的jobConfig
        jobReader.setPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));
        jobReader.setJobPluginCollector(jobPluginCollector);
        jobReader.init();

        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobReader;
    }

    /**
     * writer job的初始化，返回Writer.Job
     *
     * @return
     */
    private Writer.Job initJobWriter(
            JobPluginCollector jobPluginCollector) {
        this.writerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));

        Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(
                PluginType.WRITER, this.writerPluginName);

        // 设置writer的jobConfig
        jobWriter.setPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));
        jobWriter.setJobPluginCollector(jobPluginCollector);
        jobWriter.init();
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return jobWriter;
    }

    private void prepareJobReader() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        LOG.info(String.format("DataX Reader.Job [%s] do prepare work .",
                this.readerPluginName));
        this.jobReader.prepare();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void prepareJobWriter() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));
        LOG.info(String.format("DataX Writer.Job [%s] do prepare work .",
                this.writerPluginName));
        this.jobWriter.prepare();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    // TODO: 如果源头就是空数据
    private List<Configuration> doReaderSplit(int adviceNumber) {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        List<Configuration> readerSlicesConfigs =
                this.jobReader.split(adviceNumber);
        if (readerSlicesConfigs == null || readerSlicesConfigs.size() <= 0) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    "reader切分的task数目不能小于等于0");
        }
        LOG.info("DataX Reader.Job [{}] splits to [{}] tasks.",
                this.readerPluginName, readerSlicesConfigs.size());
        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return readerSlicesConfigs;
    }

    private List<Configuration> doWriterSplit(int readerTaskNumber) {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));

        List<Configuration> writerSlicesConfigs = this.jobWriter
                .split(readerTaskNumber);
        if (writerSlicesConfigs == null || writerSlicesConfigs.size() <= 0) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    "writer切分的task不能小于等于0");
        }
        LOG.info("DataX Writer.Job [{}] splits to [{}] tasks.",
                this.writerPluginName, writerSlicesConfigs.size());
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return writerSlicesConfigs;
    }

    /**
     * 按顺序整合reader和writer的配置，这里的顺序不能乱！ 输入是reader、writer级别的配置，输出是一个完整task的配置
     *
     * @param readerTasksConfigs
     * @param writerTasksConfigs
     * @return
     */
    private List<Configuration> mergeReaderAndWriterTaskConfigs(
            List<Configuration> readerTasksConfigs,
            List<Configuration> writerTasksConfigs) {
        if (readerTasksConfigs.size() != writerTasksConfigs.size()) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    String.format("reader切分的task数目[%d]不等于writer切分的task数目[%d].",
                            readerTasksConfigs.size(), writerTasksConfigs.size()));
        }

        List<Configuration> contentConfigs = new ArrayList<Configuration>();
        for (int i = 0; i < readerTasksConfigs.size(); i++) {
            Configuration taskConfig = Configuration.newDefault();
            taskConfig.set(CoreConstant.JOB_READER_NAME,
                    this.readerPluginName);
            taskConfig.set(CoreConstant.JOB_READER_PARAMETER,
                    readerTasksConfigs.get(i));
            taskConfig.set(CoreConstant.JOB_WRITER_NAME,
                    this.writerPluginName);
            taskConfig.set(CoreConstant.JOB_WRITER_PARAMETER,
                    writerTasksConfigs.get(i));
            taskConfig.set(CoreConstant.JOB_TASK_ID, i);
            contentConfigs.add(taskConfig);
        }

        return contentConfigs;
    }

    /**
     * 这里比较复杂，分两步整合 1. tasks到channel 2. channel到taskGroup
     * 合起来考虑，其实就是把tasks整合到taskGroup中，需要满足计算出的channel数，同时不能多起channel
     * <p/>
     * example:
     * <p/>
     * 前提条件： 切分后是1024个分表，假设用户要求总速率是1000M/s，每个channel的速率的3M/s，
     * 每个taskGroup负责运行7个channel
     * <p/>
     * 计算： 总channel数为：1000M/s / 3M/s =
     * 333个，为平均分配，计算可知有308个每个channel有3个tasks，而有25个每个channel有4个tasks，
     * 需要的taskGroup数为：333 / 7 =
     * 47...4，也就是需要48个taskGroup，47个是每个负责7个channel，有4个负责1个channel
     * <p/>
     * 处理：我们先将这负责4个channel的taskGroup处理掉，逻辑是：
     * 先按平均为3个tasks找4个channel，设置taskGroupId为0，
     * 接下来就像发牌一样轮询分配task到剩下的包含平均channel数的taskGroup中
     *
     * @param averTaskPerChannel
     * @param channelNumber
     * @param channelsPerTaskGroup
     * @return 每个taskGroup独立的全部配置
     */
    @SuppressWarnings("serial")
    private List<Configuration> distributeTasksToTaskGroup(
            int averTaskPerChannel, int channelNumber,
            int channelsPerTaskGroup) {
        Validate.isTrue(averTaskPerChannel > 0 && channelNumber > 0
                        && channelsPerTaskGroup > 0,
                "每个channel的平均task数[averTaskPerChannel]，channel数目[channelNumber]，每个taskGroup的平均channel数[channelsPerTaskGroup]都应该为正数");
        List<Configuration> taskConfigs = this.configuration
                .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
        int taskGroupNumber = channelNumber / channelsPerTaskGroup;
        int leftChannelNumber = channelNumber % channelsPerTaskGroup;
        if (leftChannelNumber > 0) {
            taskGroupNumber += 1;
        }

        /**
         * 如果只有一个taskGroup，直接打标返回
         */
        if (taskGroupNumber == 1) {
            final Configuration taskGroupConfig = this.configuration.clone();
            /**
             * configure的clone不能clone出
             */
            taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, this.configuration
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT));
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                    channelNumber);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 0);
            return new ArrayList<Configuration>() {
                {
                    add(taskGroupConfig);
                }
            };
        }

        List<Configuration> taskGroupConfigs = new ArrayList<Configuration>();
        /**
         * 将每个taskGroup中content的配置清空
         */
        for (int i = 0; i < taskGroupNumber; i++) {
            Configuration taskGroupConfig = this.configuration.clone();
            List<Configuration> taskGroupJobContent = taskGroupConfig
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
            taskGroupJobContent.clear();
            taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, taskGroupJobContent);

            taskGroupConfigs.add(taskGroupConfig);
        }

        int taskConfigIndex = 0;
        int channelIndex = 0;
        int taskGroupConfigIndex = 0;

        /**
         * 先处理掉taskGroup包含channel数不是平均值的taskGroup
         */
        if (leftChannelNumber > 0) {
            Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
            for (; channelIndex < leftChannelNumber; channelIndex++) {
                for (int i = 0; i < averTaskPerChannel; i++) {
                    List<Configuration> taskGroupJobContent = taskGroupConfig
                            .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
                    taskGroupJobContent.add(taskConfigs.get(taskConfigIndex++));
                    taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT,
                            taskGroupJobContent);
                }
            }

            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                    leftChannelNumber);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID,
                    taskGroupConfigIndex++);
        }

        /**
         * 下面需要轮询分配，并打上channel数和taskGroupId标记
         */
        int equalDivisionStartIndex = taskGroupConfigIndex;
        for (; taskConfigIndex < taskConfigs.size()
                && equalDivisionStartIndex < taskGroupConfigs.size(); ) {
            for (taskGroupConfigIndex = equalDivisionStartIndex; taskGroupConfigIndex < taskGroupConfigs
                    .size() && taskConfigIndex < taskConfigs.size(); taskGroupConfigIndex++) {
                Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
                List<Configuration> taskGroupJobContent = taskGroupConfig
                        .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
                taskGroupJobContent.add(taskConfigs.get(taskConfigIndex++));
                taskGroupConfig.set(
                        CoreConstant.DATAX_JOB_CONTENT, taskGroupJobContent);
            }
        }

        for (taskGroupConfigIndex = equalDivisionStartIndex;
             taskGroupConfigIndex < taskGroupConfigs.size(); ) {
            Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                    channelsPerTaskGroup);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID,
                    taskGroupConfigIndex++);
        }

        return taskGroupConfigs;
    }

    private void postJobReader() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        LOG.info("DataX Reader.Job [{}] do post work.",
                this.readerPluginName);
        this.jobReader.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void postJobWriter() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));
        LOG.info("DataX Writer.Job [{}] do post work.",
                this.writerPluginName);
        this.jobWriter.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    /**
     * 检查最终结果是否超出阈值，如果阈值设定小于1，则表示百分数阈值，大于1表示条数阈值。
     *
     * @param
     */
    private void checkLimit() {
        Communication communication = super.getContainerCollector().collect();
        errorLimit.checkRecordLimit(communication);
        errorLimit.checkPercentageLimit(communication);
    }
}
