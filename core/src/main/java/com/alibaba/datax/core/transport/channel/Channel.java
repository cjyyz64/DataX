package com.alibaba.datax.core.transport.channel;

import java.util.Collection;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.metric.Metric;
import com.alibaba.datax.core.statistics.metric.MetricManager;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.SleepQuiet;
import com.alibaba.datax.core.util.Status;

/**
 * 统计和限速都在这里
 * 
 */
public abstract class Channel {

	private static final Logger LOGGER = LoggerFactory.getLogger(Channel.class);

	protected int channelId;

	protected int slaveId;

	protected int capacity;

	protected long speed; // 单位：byte/s

	protected long flowControlInterval;

	protected volatile boolean isClosed = false;

	protected Configuration configuration = null;

	private static Boolean isFirstPrint = true;

	private Metric lastMetric = new Metric();

	public Channel(final Configuration configuration) {
		int id = configuration.getInt(
				CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_ID, 0);

		if (id < 0) {
			throw new IllegalArgumentException(String.format(
					"ID [%d] must be > 0 .", id));
		}

		int capacity = configuration.getInt(
				CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY, 128);
		long speed = configuration.getLong(
				CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE,
				1024 * 1024);

		if (capacity <= 0) {
			throw new IllegalArgumentException(String.format(
					"Capacity [%d] must be > 0 .", capacity));

		}

		synchronized (isFirstPrint) {
			if (isFirstPrint) {
				Channel.LOGGER.info("Channel set speed limit to " + speed
						+ (speed <= 0 ? " , No Limit actived ." : " ."));
				isFirstPrint = false;
			}
		}

		this.channelId = id;
		this.slaveId = configuration
				.getInt(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID);
		this.capacity = capacity;
		this.speed = speed;
		this.flowControlInterval = configuration.getLong(
				CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_FLOWCONTROLINTERVAL,
				1000);

		this.configuration = configuration;
	}

	public void close() {
		this.isClosed = true;
	}

	public void open() {
		this.isClosed = false;
	}

	public boolean isClosed() {
		return isClosed;
	}

	public int getChannelId() {
		return channelId;
	}

	public int getSlaveId() {
		return this.slaveId;
	}

	public int getCapacity() {
		return capacity;
	}

	public long getSpeed() {
		return speed;
	}

	public Configuration getConfiguration() {
		return this.configuration;
	}

	public void push(final Record r) {
		Validate.notNull(r, "Record cannot null .");
		this.doPush(r);
		this.statPush(this.getChannelMetric(), 1L, r.getByteSize());
	}

	public void pushAll(final Collection<Record> rs) {
		Validate.notNull(rs);
		Validate.noNullElements(rs);
		this.doPushAll(rs);
		this.statPush(this.getChannelMetric(), rs.size(), this.getByteSize(rs));
	}

	public Record pull() {
		Record record = this.doPull();
		this.statPull(this.getChannelMetric(), 1L, record.getByteSize());
		return record;
	}

	public void pullAll(final Collection<Record> rs) {
		Validate.notNull(rs);
		this.doPullAll(rs);
		this.statPull(this.getChannelMetric(), rs.size(), this.getByteSize(rs));
	}

	public void decreaseTerminateRecordMetric() {
		Metric metric = this.getChannelMetric();
		metric.setReadSucceedRecords(metric.getReadSucceedRecords() - 1);
		metric.setWriteReceivedRecords(metric.getWriteReceivedRecords() - 1);
		metric.setStage(metric.getStage() + 1);
	}

	protected abstract void doPush(Record r);

	protected abstract void doPushAll(Collection<Record> rs);

	protected abstract Record doPull();

	protected abstract void doPullAll(Collection<Record> rs);

	public abstract int size();

	public abstract boolean isEmpty();

	public Metric getChannelMetric() {
		return MetricManager.getChannelMetric(getSlaveId(), getChannelId());
	}

	private long getByteSize(final Collection<Record> rs) {
		long size = 0;
		for (final Record each : rs) {
			size += each.getByteSize();
		}
		return size;
	}

	private void statPush(Metric metric, long recordSize, long byteSize) {
		metric.incrReadSucceedRecords(recordSize);
		metric.incrReadSucceedBytes(byteSize);

		boolean isChannelSpeedLimit = (this.speed > 0);
		if (!isChannelSpeedLimit) {
			return;
		}

		long lastTimestamp = lastMetric.getTimeStamp();
		long nowTimestamp = System.currentTimeMillis();
		long interval = nowTimestamp - lastTimestamp;
		if (interval - this.flowControlInterval >= 0) {
			if (lastTimestamp > 0) {
				long currentSpeed = (metric.getTotalReadBytes() - lastMetric
						.getTotalReadBytes()) * 1000 / interval;
				if (currentSpeed > this.speed) {
					/**
					 * 计算休眠时间，使平均速度为this.speed
					 */
					long sleepTime = currentSpeed * interval / this.speed
							- interval;
					SleepQuiet.sleep(sleepTime);
				}
			}
			lastMetric.setReadSucceedBytes(metric.getReadSucceedBytes());
			lastMetric.setReadFailedBytes(metric.getReadFailedBytes());
			lastMetric.setTimeStamp(nowTimestamp);
		}
	}

	private void statPull(Metric metric, long recordSize, long byteSize) {
		metric.incrWriteReceivedRecords(recordSize);
		metric.incrWriteReceivedBytes(byteSize);
	}
}
