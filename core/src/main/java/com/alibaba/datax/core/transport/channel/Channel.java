package com.alibaba.datax.core.transport.channel;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.SleepQuiet;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationManager;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 *
 * Created by jingxing on 14-8-25.
 *
 * 统计和限速都在这里
 *
 */
public abstract class Channel {

    private static final Logger LOGGER = LoggerFactory.getLogger(Channel.class);

    protected int taskGroupId;

    protected int capacity;

    protected long byteSpeed; // bps: bytes/s

    protected long recordSpeed; // tps: records/s

    protected long flowControlInterval;

    protected volatile boolean isClosed = false;

    protected Configuration configuration = null;

    private static Boolean isFirstPrint = true;

    private Communication currentCommunication;

    private Communication lastCommunication = new Communication();

    public Channel(final Configuration configuration) {
        int capacity = configuration.getInt(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY, 128);
        long byteSpeed = configuration.getLong(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE, 1024 * 1024);
        long recordSpeed = configuration.getLong(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_RECORD, 10000);

        if (capacity <= 0) {
            throw new IllegalArgumentException(String.format(
                    "通道容量[%d]必须大于0.", capacity));
        }

        synchronized (isFirstPrint) {
            if (isFirstPrint) {
                Channel.LOGGER.info("Channel set byte_speed_limit to " + byteSpeed
                        + (byteSpeed <= 0 ? ", No bps activated." : "."));
                Channel.LOGGER.info("Channel set record_speed_limit to " + recordSpeed
                        + (recordSpeed <= 0 ? ", No tps activated." : "."));
                isFirstPrint = false;
            }
        }

        this.taskGroupId = configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
        this.capacity = capacity;
        this.byteSpeed = byteSpeed;
        this.recordSpeed = recordSpeed;
        this.flowControlInterval = configuration.getLong(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_FLOWCONTROLINTERVAL, 1000);

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

    public int getTaskGroupId() {
        return this.taskGroupId;
    }

    public int getCapacity() {
        return capacity;
    }

    public long getByteSpeed() {
        return byteSpeed;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public void setCommunication(final Communication communication) {
        this.currentCommunication = communication;
        this.lastCommunication.reset();
    }

    public void push(final Record r) {
        Validate.notNull(r, "record不能为空.");
        this.doPush(r);
        this.statPush(1L, r.getByteSize());
    }

    public void pushAll(final Collection<Record> rs) {
        Validate.notNull(rs);
        Validate.noNullElements(rs);
        this.doPushAll(rs);
        this.statPush(rs.size(), this.getByteSize(rs));
    }

    public Record pull() {
        Record record = this.doPull();
        this.statPull(1L, record.getByteSize());
        return record;
    }

    public void pullAll(final Collection<Record> rs) {
        Validate.notNull(rs);
        this.doPullAll(rs);
        this.statPull(rs.size(), this.getByteSize(rs));
    }

    public void decreaseTerminateRecordMetric() {
        currentCommunication.setLongCounter(CommunicationManager.READ_SUCCEED_RECORDS,
                currentCommunication.getLongCounter(CommunicationManager.READ_SUCCEED_RECORDS) - 1);
        currentCommunication.setLongCounter(CommunicationManager.WRITE_RECEIVED_RECORDS,
                currentCommunication.getLongCounter(CommunicationManager.WRITE_RECEIVED_RECORDS) - 1);
        currentCommunication.setLongCounter(CommunicationManager.STAGE,
                currentCommunication.getLongCounter(CommunicationManager.STAGE) + 1);
    }

    protected abstract void doPush(Record r);

    protected abstract void doPushAll(Collection<Record> rs);

    protected abstract Record doPull();

    protected abstract void doPullAll(Collection<Record> rs);

    public abstract int size();

    public abstract boolean isEmpty();

    private long getByteSize(final Collection<Record> rs) {
        long size = 0;
        for (final Record each : rs) {
            size += each.getByteSize();
        }
        return size;
    }

    private void statPush(long recordSize, long byteSize) {
        currentCommunication.increaseCounter(CommunicationManager.READ_SUCCEED_RECORDS,
                recordSize);
        currentCommunication.increaseCounter(CommunicationManager.READ_SUCCEED_BYTES,
                byteSize);

        boolean isChannelByteSpeedLimit = (this.byteSpeed > 0);
        boolean isChannelRecordSpeedLimit = (this.recordSpeed > 0);
        if (!isChannelByteSpeedLimit && !isChannelRecordSpeedLimit) {
            return;
        }

        long lastTimestamp = lastCommunication.getTimestamp();
        long nowTimestamp = System.currentTimeMillis();
        long interval = nowTimestamp - lastTimestamp;
        if (interval - this.flowControlInterval >= 0) {
            long byteLimitSleepTime = 0;
            long recordLimitSleepTime = 0;
            if(isChannelByteSpeedLimit) {
                long currentByteSpeed = (CommunicationManager.getTotalReadBytes(currentCommunication) -
                        CommunicationManager.getTotalReadBytes(lastCommunication)) * 1000 / interval;
                if (currentByteSpeed > this.byteSpeed) {
                    // 计算根据byteLimit得到的休眠时间
                    byteLimitSleepTime = currentByteSpeed * interval / this.byteSpeed
                            - interval;
                }
            }

            if(isChannelRecordSpeedLimit) {
                long currentRecordSpeed = (CommunicationManager.getTotalReadRecords(currentCommunication) -
                        CommunicationManager.getTotalReadRecords(lastCommunication)) * 1000 / interval;
                if(currentRecordSpeed > this.recordSpeed) {
                    // 计算根据recordLimit得到的休眠时间
                    recordLimitSleepTime = currentRecordSpeed * interval / this.recordSpeed
                            - interval;
                }
            }

            // 休眠时间取较大值
            long sleepTime = byteLimitSleepTime<recordLimitSleepTime ?
                    recordLimitSleepTime : byteLimitSleepTime;
            if(sleepTime > 0) {
                SleepQuiet.sleep(sleepTime);
            }

            lastCommunication.setLongCounter(CommunicationManager.READ_SUCCEED_BYTES,
                    currentCommunication.getLongCounter(CommunicationManager.READ_SUCCEED_BYTES));
            lastCommunication.setLongCounter(CommunicationManager.READ_FAILED_BYTES,
                    currentCommunication.getLongCounter(CommunicationManager.READ_FAILED_BYTES));
            lastCommunication.setLongCounter(CommunicationManager.READ_SUCCEED_RECORDS,
                    currentCommunication.getLongCounter(CommunicationManager.READ_SUCCEED_RECORDS));
            lastCommunication.setLongCounter(CommunicationManager.READ_FAILED_RECORDS,
                    currentCommunication.getLongCounter(CommunicationManager.READ_FAILED_RECORDS));
            lastCommunication.setTimestamp(nowTimestamp);
        }
    }

    private void statPull(long recordSize, long byteSize) {
        currentCommunication.increaseCounter(
                CommunicationManager.WRITE_RECEIVED_RECORDS, recordSize);
        currentCommunication.increaseCounter(
                CommunicationManager.WRITE_RECEIVED_BYTES, byteSize);
    }
}
