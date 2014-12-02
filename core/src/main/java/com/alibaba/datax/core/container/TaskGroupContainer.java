package com.alibaba.datax.core.container;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.core.statistics.collector.plugin.task.AbstractTaskPluginCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.runner.AbstractRunner;
import com.alibaba.datax.core.container.runner.ReaderRunner;
import com.alibaba.datax.core.container.runner.WriterRunner;
import com.alibaba.datax.core.container.util.LoadUtil;
import com.alibaba.datax.core.statistics.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordExchanger;
import com.alibaba.fastjson.JSON;

/**
 * Created by jingxing on 14-8-24.
 */
public class TaskGroupContainer extends AbstractContainer {
    private static final Logger LOG = LoggerFactory
            .getLogger(TaskGroupContainer.class);

    /**
     * 当前taskGroup所属jobId
     */
    private long jobId;

    /**
     * 当前taskGroupId
     */
    private int taskGroupId;

    /**
     * 使用的channel类
     */
    private String channelClazz;

    /**
     * task收集器使用的类
     */
    private String taskCollectorClass;

    public TaskGroupContainer(Configuration configuration) {
        super(configuration);
        super.setContainerCollector(ClassUtil.instantiate(
                configuration.getString(
                        CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_CONTAINER_TASKGROUPCLASS),
                AbstractContainerCollector.class, configuration));
        this.jobId = this.configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        this.taskGroupId = this.configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);

        this.channelClazz = this.configuration.getString(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CLASS);
        this.taskCollectorClass = this.configuration.getString(
                CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS);
    }

    public long getJobId() {
        return jobId;
    }

    public int getTaskGroupId() {
        return taskGroupId;
    }

    @Override
    public void start() {
        Communication taskGroupCommunication;

        try {
            List<Configuration> taskConfigs = this.configuration
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);

            LOG.debug("taskGroup[{}]'s task configs[{}]", this.taskGroupId,
                    JSON.toJSONString(taskConfigs));

            /**
             * 状态check时间间隔，较短，可以把任务及时分发到对应channel中
             */
            int sleepIntervalInMillSec = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_SLEEPINTERVAL, 100);
            /**
             * 状态汇报时间间隔，稍长，避免大量汇报
             */
            long reportIntervalInMillSec = this.configuration.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_REPORTINTERVAL,
                    5000);

            // 获取channel数目
            int channelNumber = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);

            // 根据channelNumber初始化taskExecutors的大小
            List<TaskExecutor> taskExecutors = new ArrayList<TaskExecutor>();
            for (int i = 0; i < channelNumber; i++) {
                taskExecutors.add(null);
            }
            this.containerCollector.registerCommunication(taskConfigs);

            LOG.info(String.format(
                    "taskGroupId=[%d] start [%d] channels for [%d] tasks.",
                    this.taskGroupId, channelNumber, taskConfigs.size()));

            long lastReportTimeStamp = 0;
            int taskIndex = 0;
            while (true) {
                State taskExecutorTotalState = this.containerCollector.collectState();
                // 发现该taskGroup下taskExecutor的总状态失败则汇报错误
                if(taskExecutorTotalState.isFailed()) {
                    taskGroupCommunication = this.containerCollector.collect();
                    this.containerCollector.report(taskGroupCommunication);

                    throw DataXException.asDataXException(
                            FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, taskGroupCommunication.getThrowable());
                }

                for(int slotIndex=0; slotIndex<taskExecutors.size(); slotIndex++) {
                    TaskExecutor taskExecutor = taskExecutors.get(slotIndex);
                    // 当taskExecutor为空或上一个任务已完成，且仍有未完成任务时，启动一个新的taskExecutor
                    if((taskExecutor==null || taskExecutor.isTaskFinished())
                            && taskIndex<taskConfigs.size()) {
                        LOG.debug(String.format("start a new taskExecutor[%d]", taskIndex));
                        TaskExecutor newTaskExecutor = new TaskExecutor(
                                taskConfigs.get(taskIndex++));
                        // 将新生成的newTaskExecutor放入taskExecutors中，并启动
                        taskExecutors.set(slotIndex, newTaskExecutor);
                        newTaskExecutor.doStart();
                    }
                }

                boolean isAllTaskDone = true;
                for (TaskExecutor executor : taskExecutors) {
                    if (!executor.isTaskFinished()) {
                        isAllTaskDone = false;
                        break;
                    }
                }

                if (taskIndex >= taskConfigs.size() && isAllTaskDone
                        && taskExecutorTotalState.isSucceed()) {
                    LOG.info("taskGroup[{}] complete all tasks.", this.taskGroupId);
                    break;
                }

                // 如果当前时间已经超出汇报时间的interval，那么我们需要马上汇报
                long now = System.currentTimeMillis();
                if (now - lastReportTimeStamp > reportIntervalInMillSec) {
                    taskGroupCommunication = this.containerCollector.collect();
                    this.containerCollector.report(taskGroupCommunication);
                    lastReportTimeStamp = now;
                }

                Thread.sleep(sleepIntervalInMillSec);
            }

            //最后还要汇报一次
            taskGroupCommunication = this.containerCollector.collect();
            this.containerCollector.report(taskGroupCommunication);
        } catch (Throwable e) {
            taskGroupCommunication = this.containerCollector.collect();

            if(taskGroupCommunication.getThrowable() == null) {
                taskGroupCommunication.setThrowable(e);
            }
            taskGroupCommunication.setState(State.FAIL);
            this.containerCollector.report(taskGroupCommunication);

            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }

    /**
     * TaskExecutor是一个完整task的执行器
     * 其中包括1：1的reader和writer
     */
    class TaskExecutor {
        private Configuration taskConfig;

        private int taskId;

        private Channel channel;

        private Thread readerThread;

        private Thread writerThread;

        /**
         * 该处的taskCommunication在多处用到：
         * 1. channel
         * 2. readerRunner和writerRunner
         * 3. reader和writer的taskPluginCollector
         */
        private Communication taskCommunication;

        public TaskExecutor(Configuration taskConf) {
            // 获取该taskExecutor的配置
            this.taskConfig = taskConf;
            org.apache.commons.lang.Validate.isTrue(null != this.taskConfig.getConfiguration(CoreConstant.JOB_READER)
                            && null != this.taskConfig.getConfiguration(CoreConstant.JOB_WRITER),
                    "[reader|writer]的插件参数不能为空!");

            // 得到taskId
            this.taskId = this.taskConfig.getInt(CoreConstant.JOB_TASK_ID);

            /**
             * 由taskId得到该taskExecutor的Communication
             * 要传给readerRunner和writerRunner，同时要传给channel作统计用
             */
            this.taskCommunication = containerCollector
                    .getCommunication(taskId);
            org.apache.commons.lang.Validate.notNull(
                    this.taskCommunication,
                    String.format("taskId[%d]的Communication没有注册过", taskId));
            this.channel = ClassUtil.instantiate(channelClazz,
                    Channel.class, configuration);
            this.channel.setCommunication(this.taskCommunication);

            /**
             * 生成writerThread
             */
            WriterRunner writerRunner = (WriterRunner) generateRunner(false);
            this.writerThread = new Thread(writerRunner,
                    String.format("%d-%d-%d-writer",
                            jobId, taskGroupId, this.taskId));
            //通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
            this.writerThread.setContextClassLoader(LoadUtil.getJarLoader(
                    PluginType.WRITER, this.taskConfig.getString(
                            CoreConstant.JOB_WRITER_NAME)));

            /**
             * 生成readerThread
             */
            ReaderRunner readerRunner = (ReaderRunner) generateRunner(true);
            this.readerThread = new Thread(readerRunner,
                    String.format("%d-%d-%d-reader",
                            jobId, taskGroupId, this.taskId));
            /**
             * 通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
             */
            this.readerThread.setContextClassLoader(LoadUtil.getJarLoader(
                    PluginType.READER, this.taskConfig.getString(
                            CoreConstant.JOB_READER_NAME)));
        }

        public void doStart() {
            this.writerThread.start();

            // bug: 如果启动伊始，writer即挂，这里需要判断
            while (!this.writerThread.isAlive()) {
                if (this.taskCommunication.getState().isFailed()) {
                    throw DataXException.asDataXException(
                            FrameworkErrorCode.RUNTIME_ERROR,
                            this.taskCommunication.getThrowable());
                } else {
                    break;
                }
            }

            this.readerThread.start();

            // 我们担心在isTaskDone函数检查Reader/Writer 是否存活时候存在时序问题，因此这里先优先保证启动成功
            while (!this.readerThread.isAlive()) {
                // 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
                if (this.taskCommunication.getState().isFailed()) {
                    throw DataXException.asDataXException(
                            FrameworkErrorCode.RUNTIME_ERROR,
                            this.taskCommunication.getThrowable());
                } else {
                    break;
                }
            }
        }

        private AbstractRunner generateRunner(boolean isReader) {
            AbstractRunner newRunner = null;
            if(isReader) {
                newRunner = LoadUtil.loadPluginRunner(PluginType.READER,
                        this.taskConfig.getString(CoreConstant.JOB_READER_NAME));
                newRunner.setJobConf(this.taskConfig.getConfiguration(
                        CoreConstant.JOB_READER_PARAMETER));
                ((ReaderRunner)newRunner).setRecordSender(
                        new BufferedRecordExchanger(this.channel));
                /**
                 * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                 */
                newRunner.setTaskPluginCollector(ClassUtil.instantiate(
                        taskCollectorClass, AbstractTaskPluginCollector.class,
                        configuration, this.taskCommunication,
                        PluginType.READER));
            } else {
                newRunner = LoadUtil.loadPluginRunner(PluginType.WRITER,
                        this.taskConfig.getString(CoreConstant.JOB_WRITER_NAME));
                newRunner.setJobConf(this.taskConfig
                        .getConfiguration(CoreConstant.JOB_WRITER_PARAMETER));
                ((WriterRunner)newRunner).setRecordReceiver(new BufferedRecordExchanger(
                        this.channel));
                /**
                 * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                 */
                newRunner.setTaskPluginCollector(ClassUtil.instantiate(
                        taskCollectorClass, AbstractTaskPluginCollector.class,
                        configuration, this.taskCommunication,
                        PluginType.WRITER));
            }

            newRunner.setTaskGroupId(taskGroupId);
            newRunner.setTaskId(this.taskId);
            newRunner.setRunnerCommunication(this.taskCommunication);

            return newRunner;
        }

        // 检查reader、writer线程是否完成工作
        private boolean isTaskFinished() {
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
