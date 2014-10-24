package com.alibaba.datax.core.util;

import org.apache.commons.lang.StringUtils;

import java.io.File;

/**
 * Created by jingxing on 14-8-25.
 */
public class CoreConstant {
	// --------------------------- 全局使用的变量(最好按照逻辑顺序，调整下成员变量顺序)
	// --------------------------------

	public static final String DATAX_CORE_CONTAINER_SLAVE_CHANNEL = "core.container.slave.channel";

	public static final String DATAX_CORE_CONTAINER_MODEL = "core.container.model";

	public static final String DATAX_CORE_CONTAINER_MASTER_ID = "core.container.master.id";

	public static final String DATAX_CORE_CONTAINER_MASTER_REPORTINTERVAL = "core.container.master.reportInterval";

	public static final String DATAX_CORE_CONTAINER_MASTER_CLASS = "core.container.master.class";

	public static final String DATAX_CORE_CONTAINER_SLAVE_CLASS = "core.container.slave.class";

	public static final String DATAX_CORE_CONTAINER_SLAVE_ID = "core.container.slave.id";

	public static final String DATAX_CORE_CONTAINER_SLAVE_SLEEPINTERVAL = "core.container.slave.sleepInterval";

	public static final String DATAX_CORE_CONTAINER_SLAVE_REPORTINTERVAL = "core.container.slave.reportInterval";

    public static final String DATAX_CORE_CLUSTERMANAGER_ADDRESS = "core.clusterManager.address";

    public static final String DATAX_CORE_CLUSTERMANAGER_TIMEOUT = "core.clusterManager.timeout";

	public static final String DATAX_CORE_SCHEDULER_CLASS = "core.scheduler.class";

	public static final String DATAX_CORE_TRANSPORT_CHANNEL_CLASS = "core.transport.channel.class";

	public static final String DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY = "core.transport.channel.capacity";

	public static final String DATAX_CORE_TRANSPORT_CHANNEL_ID = "core.transport.channel.id";

	public static final String DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE = "core.transport.channel.speed.byte";

	public static final String DATAX_CORE_TRANSPORT_CHANNEL_FLOWCONTROLINTERVAL = "core.transport.channel.flowControlInterval";

	public static final String DATAX_CORE_TRANSPORT_EXCHANGER_BUFFERSIZE = "core.transport.exchanger.bufferSize";

	public static final String DATAX_CORE_TRANSPORT_RECORD_CLASS = "core.transport.record.class";

	public static final String DATAX_CORE_STATISTICS_COLLECTOR_CONTAINER_MASTERCLASS = "core.statistics.collector.container.masterClass";

	public static final String DATAX_CORE_STATISTICS_COLLECTOR_CONTAINER_SLAVECLASS = "core.statistics.collector.container.slaveClass";

	public static final String DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_SLAVECLASS = "core.statistics.collector.plugin.slaveClass";

	public static final String DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_MAXDIRTYNUM = "core.statistics.collector.plugin.maxDirtyNumber";

	public static final String DATAX_JOB_CONTENT_READER_NAME = "job.content[0].reader.name";

	public static final String DATAX_JOB_CONTENT_READER_PARAMETER = "job.content[0].reader.parameter";

	public static final String DATAX_JOB_CONTENT_WRITER_NAME = "job.content[0].writer.name";

	public static final String DATAX_JOB_CONTENT_WRITER_PARAMETER = "job.content[0].writer.parameter";

	public static final String DATAX_JOB_CONTENT = "job.content";

	public static final String DATAX_JOB_SETTING_SPEED_BYTE = "job.setting.speed.byte";

	public static final String DATAX_JOB_SETTING_SPEED_CHANNEL = "job.setting.speed.channel";

	public static final String DATAX_JOB_SETTING_ERRORLIMIT = "job.setting.errorLimit";

    public static final String DATAX_JOB_SETTING_ERRORLIMIT_RECORD = "job.setting.errorLimit.record";

    public static final String DATAX_JOB_SETTING_ERRORLIMIT_PERCENT = "job.setting.errorLimit.percentage";

    // ----------------------------- 局部使用的变量
    public static final String JOB_WRITER = "reader";

	public static final String JOB_READER = "reader";

	public static final String JOB_READER_NAME = "reader.name";

	public static final String JOB_READER_PARAMETER = "reader.parameter";

	public static final String JOB_WRITER_NAME = "writer.name";

	public static final String JOB_WRITER_PARAMETER = "writer.parameter";

	public static final String JOB_SLICEID = "sliceId";

	// ----------------------------- 环境变量 ---------------------------------

	public static String DATAX_HOME = System.getProperty("datax.home");

	public static String DATAX_CONF_PATH = StringUtils.join(new String[] {
			DATAX_HOME, "conf", "core.json" }, File.separator);

	public static String DATAX_CONF_LOG_PATH = StringUtils.join(new String[] {
			DATAX_HOME, "conf", "logback.xml" }, File.separator);

	public static String DATAX_PLUGIN_HOME = StringUtils.join(new String[] {
			DATAX_HOME, "plugin" }, File.separator);

	public static String DATAX_PLUGIN_READER_HOME = StringUtils.join(
			new String[] { DATAX_HOME, "plugin", "reader" }, File.separator);

	public static String DATAX_PLUGIN_WRITER_HOME = StringUtils.join(
			new String[] { DATAX_HOME, "plugin", "writer" }, File.separator);

	public static String DATAX_BIN_HOME = StringUtils.join(new String[] {
			DATAX_HOME, "bin" }, File.separator);

	public static String DATAX_JOB_HOME = StringUtils.join(new String[] {
			DATAX_HOME, "job" }, File.separator);

}
