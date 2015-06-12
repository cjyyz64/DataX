package com.alibaba.datax.core.taskgroup;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.AbstractContainer;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.taskgroup.DistributeTGContainerCommunicator;
import com.alibaba.datax.core.statistics.container.communicator.taskgroup.LocalTGContainerCommunicator;
import com.alibaba.datax.core.statistics.container.communicator.taskgroup.StandaloneTGContainerCommunicator;
import com.alibaba.datax.core.statistics.plugin.task.AbstractTaskPluginCollector;
import com.alibaba.datax.core.taskgroup.runner.AbstractRunner;
import com.alibaba.datax.core.taskgroup.runner.ReaderRunner;
import com.alibaba.datax.core.taskgroup.runner.WriterRunner;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordExchanger;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.ExecuteMode;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.fastjson.JSON;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

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

        initCommunicator(configuration);

        this.jobId = this.configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        this.taskGroupId = this.configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);

        this.channelClazz = this.configuration.getString(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CLASS);
        this.taskCollectorClass = this.configuration.getString(
                CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS);
    }

    private void initCommunicator(Configuration configuration) {
        //TODO 修改 communicator的factory
        String executeMode = configuration.getString(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE);
        if (ExecuteMode.isLocal(executeMode)) {
            super.setContainerCommunicator(new LocalTGContainerCommunicator(configuration));
        } else if (ExecuteMode.isDistribute(executeMode)) {
            super.setContainerCommunicator(new DistributeTGContainerCommunicator(configuration));
        } else {
            super.setContainerCommunicator(new StandaloneTGContainerCommunicator(configuration));
        }

    }

    public long getJobId() {
        return jobId;
    }

    public int getTaskGroupId() {
        return taskGroupId;
    }

    @Override
    public void start() {
        Communication nowTaskGroupContainerCommunication = null;

        try {
            
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
            
            List<Configuration> taskConfigs = this.configuration
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);

            if(LOG.isDebugEnabled()) {
                LOG.debug("taskGroup[{}]'s task configs[{}]", this.taskGroupId,
                        JSON.toJSONString(taskConfigs));
            }
            
            int taskCountInThisTaskGroup = taskConfigs.size();
            LOG.info(String.format(
                    "taskGroupId=[%d] start [%d] channels for [%d] tasks.",
                    this.taskGroupId, channelNumber, taskCountInThisTaskGroup));
            
            this.containerCommunicator.registerCommunication(taskConfigs);
            
            Map<Integer, Configuration> taskConfigMap = buildTaskConfigMap(taskConfigs);
            Queue<Configuration> taskQueue = buildRemainTasks(taskConfigs);
            List<TaskExecutor> runTasks = new ArrayList<TaskExecutor>(channelNumber);

            long lastReportTimeStamp = 0;
            Communication lastTaskGroupContainerCommunication = new Communication();

            while (true) {
            	//1.判断是否已有任务失败
            	boolean failedOrKilled = false;
            	Map<Integer, Communication> communicationMap = containerCommunicator.getCommunicationMap();
            	for(Map.Entry<Integer, Communication> entry : communicationMap.entrySet()){
            		Integer taskId = entry.getKey();
            		Communication taskCommunication = entry.getValue();
            		if(taskCommunication.getState() == State.FAILED){ //失败
            			TaskExecutor taskExecutor = removeTask(runTasks, taskId);
            			if(taskExecutor.supportFailOver()){
            				Configuration taskConfig = taskConfigMap.get(taskId);
            				taskQueue.offer(taskConfig); //重新加入任务列表
            			}else{
            				failedOrKilled = true;
                			break;
            			}
            		}else if(taskCommunication.getState() == State.KILLED){
            			failedOrKilled = true;
            			break;
            		}
            	}
            	
                // 2.发现该taskGroup下taskExecutor的总状态失败则汇报错误
                if (failedOrKilled) {
                    nowTaskGroupContainerCommunication = this.containerCommunicator.collect();

                    Communication reportCommunication = CommunicationTool.getReportCommunication(nowTaskGroupContainerCommunication, lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
                    nowTaskGroupContainerCommunication.setTimestamp(System.currentTimeMillis());
                    this.containerCommunicator.report(reportCommunication);

                    throw DataXException.asDataXException(
                            FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, reportCommunication.getThrowable());
                }
                
                //3.有任务未执行，且正在运行的任务数小于最大通道限制
                while(!taskQueue.isEmpty() && runTasks.size() < channelNumber){
                	Configuration taskConfig = taskQueue.poll();
                	TaskExecutor taskExecutor = new TaskExecutor(taskConfig.clone());
                	taskExecutor.doStart();
                	runTasks.add(taskExecutor);
                }

                //4.任务列表为空，搜集状态为success，且executor已结束--->成功
                if (taskQueue.isEmpty() && containerCommunicator.collectState() == State.SUCCEEDED && isAllTaskDone(runTasks)) {
                	// 成功的情况下，也需要汇报一次。否则在任务结束非常快的情况下，采集的信息将会不准确
                    nowTaskGroupContainerCommunication = this.containerCommunicator.collect();
                    nowTaskGroupContainerCommunication.setTimestamp(System.currentTimeMillis());

                    Communication reportCommunication = CommunicationTool.getReportCommunication(nowTaskGroupContainerCommunication, lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
                    this.containerCommunicator.report(reportCommunication);

                    LOG.info("taskGroup[{}] completed it's tasks.", this.taskGroupId);
                    break;
                }

                // 5.如果当前时间已经超出汇报时间的interval，那么我们需要马上汇报
                long now = System.currentTimeMillis();
                if (now - lastReportTimeStamp > reportIntervalInMillSec) {
                    nowTaskGroupContainerCommunication = this.containerCommunicator.collect();
                    nowTaskGroupContainerCommunication.setTimestamp(System.currentTimeMillis());

                    Communication reportCommunication = CommunicationTool.getReportCommunication(nowTaskGroupContainerCommunication, lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
                    this.containerCommunicator.report(reportCommunication);

                    lastReportTimeStamp = now;
                }

                lastTaskGroupContainerCommunication = nowTaskGroupContainerCommunication;
                Thread.sleep(sleepIntervalInMillSec);
            }

            //6.最后还要汇报一次
            nowTaskGroupContainerCommunication = this.containerCommunicator.collect();
            nowTaskGroupContainerCommunication.setTimestamp(System.currentTimeMillis());

            Communication reportCommunication = CommunicationTool.getReportCommunication(nowTaskGroupContainerCommunication, lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
            this.containerCommunicator.report(reportCommunication);
        } catch (Throwable e) {
            nowTaskGroupContainerCommunication = this.containerCommunicator.collect();

            if (nowTaskGroupContainerCommunication.getThrowable() == null) {
                nowTaskGroupContainerCommunication.setThrowable(e);
            }
            nowTaskGroupContainerCommunication.setState(State.FAILED);
            this.containerCommunicator.report(nowTaskGroupContainerCommunication);

            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }
    
    private Map<Integer, Configuration> buildTaskConfigMap(List<Configuration> configurations){
    	Map<Integer, Configuration> map = new HashMap<Integer, Configuration>();
    	for(Configuration taskConfig : configurations){
        	int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
        	map.put(taskId, taskConfig);
    	}
    	return map;
    }
    
    private Queue<Configuration> buildRemainTasks(List<Configuration> configurations){
    	Queue<Configuration> remainTasks = new LinkedList<Configuration>();
    	for(Configuration taskConfig : configurations){
    		remainTasks.offer(taskConfig);
    	}
    	return remainTasks;
    }
    
    private TaskExecutor removeTask(List<TaskExecutor> taskList, int taskId){
    	Iterator<TaskExecutor> iterator = taskList.iterator();
    	while(iterator.hasNext()){
    		TaskExecutor taskExecutor = iterator.next();
    		if(taskExecutor.getTaskId() == taskId){
    			iterator.remove();
    			return taskExecutor;
    		}
    	}
    	return null;
    }
    
    private boolean isAllTaskDone(List<TaskExecutor> taskList){
    	for(TaskExecutor taskExecutor : taskList){
    		if(!taskExecutor.isTaskFinished()){
    			return false;
    		}
    	}
    	return true;
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
        
        private ReaderRunner readerRunner;
        
        private WriterRunner writerRunner;

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
            Validate.isTrue(null != this.taskConfig.getConfiguration(CoreConstant.JOB_READER)
                            && null != this.taskConfig.getConfiguration(CoreConstant.JOB_WRITER),
                    "[reader|writer]的插件参数不能为空!");

            // 得到taskId
            this.taskId = this.taskConfig.getInt(CoreConstant.TASK_ID);

            /**
             * 由taskId得到该taskExecutor的Communication
             * 要传给readerRunner和writerRunner，同时要传给channel作统计用
             */
            this.taskCommunication = containerCommunicator
                    .getCommunication(taskId);
            Validate.notNull(this.taskCommunication,
                    String.format("taskId[%d]的Communication没有注册过", taskId));
            this.channel = ClassUtil.instantiate(channelClazz,
                    Channel.class, configuration);
            this.channel.setCommunication(this.taskCommunication);

            /**
             * 生成writerThread
             */
            writerRunner = (WriterRunner) generateRunner(PluginType.WRITER);
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
            readerRunner = (ReaderRunner) generateRunner(PluginType.READER);
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

            // reader没有起来，writer不可能结束
            if (!this.writerThread.isAlive() || this.taskCommunication.getState() == State.FAILED) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        this.taskCommunication.getThrowable());
            }

            this.readerThread.start();

            // 这里reader可能很快结束
            if (!this.readerThread.isAlive() && this.taskCommunication.getState() == State.FAILED) {
                // 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        this.taskCommunication.getThrowable());
            }
        }

        private AbstractRunner generateRunner(PluginType pluginType) {
            AbstractRunner newRunner = null;

            switch (pluginType) {
                case READER:
                    newRunner = LoadUtil.loadPluginRunner(pluginType,
                            this.taskConfig.getString(CoreConstant.JOB_READER_NAME));
                    newRunner.setJobConf(this.taskConfig.getConfiguration(
                            CoreConstant.JOB_READER_PARAMETER));
                    ((ReaderRunner) newRunner).setRecordSender(
                            new BufferedRecordExchanger(this.channel));
                    /**
                     * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                     */
                    newRunner.setTaskPluginCollector(ClassUtil.instantiate(
                            taskCollectorClass, AbstractTaskPluginCollector.class,
                            configuration, this.taskCommunication,
                            PluginType.READER));
                    break;
                case WRITER:
                    newRunner = LoadUtil.loadPluginRunner(pluginType,
                            this.taskConfig.getString(CoreConstant.JOB_WRITER_NAME));
                    newRunner.setJobConf(this.taskConfig
                            .getConfiguration(CoreConstant.JOB_WRITER_PARAMETER));
                    ((WriterRunner) newRunner).setRecordReceiver(new BufferedRecordExchanger(
                            this.channel));
                    /**
                     * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                     */
                    newRunner.setTaskPluginCollector(ClassUtil.instantiate(
                            taskCollectorClass, AbstractTaskPluginCollector.class,
                            configuration, this.taskCommunication,
                            PluginType.WRITER));
                    break;
                default:
                    throw DataXException.asDataXException(FrameworkErrorCode.ARGUMENT_ERROR, "Cant generateRunner for:" + pluginType);
            }

            newRunner.setTaskGroupId(taskGroupId);
            newRunner.setTaskId(this.taskId);
            newRunner.setRunnerCommunication(this.taskCommunication);

            return newRunner;
        }

        // 检查任务是否结束
        private boolean isTaskFinished() {
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
            
            if(taskCommunication!=null && taskCommunication.isFinished()){
        		return true;
        	}

            return true;
        }
        
        private int getTaskId(){
        	return taskId;
        }
        
        private boolean supportFailOver(){
        	return writerRunner.supportFailOver();
        }
    }
}
