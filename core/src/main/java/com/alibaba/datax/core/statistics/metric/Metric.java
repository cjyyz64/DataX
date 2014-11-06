package com.alibaba.datax.core.statistics.metric;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.beanutils.BeanUtils;

import com.alibaba.datax.common.base.BaseObject;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.Status;

/**
 * Created by jingxing on 14-8-27.
 * 
 */
public class Metric extends BaseObject implements Cloneable {
	/** slave/master的id号 */
	private long id;

	/** slave/master的ip地址 */
	private String ip;

	/**
	 * slave/master的当前处的状态
	 * */
	private Status status;

	/**
	 * 当前完成的子任务数量，主要用作计算当前进度
	 * */
	private int stage;

	/**
	 * 最近一次更新的时间戳
	 * */
	private long timeStamp;

	private Throwable throwable;

	private AtomicLong readSucceedRecords;

	private AtomicLong readSucceedBytes;

	private AtomicLong readFailedRecords;

	private AtomicLong readFailedBytes;

	private AtomicLong writeReceivedRecords;

	private AtomicLong writeReceivedBytes;

	private AtomicLong writeFailedRecords;

	private AtomicLong writeFailedBytes;

	private long byteSpeed;

	private long recordSpeed;

	private double percentage;

	private Map<String, List<String>> message;

	private static String IP = "UNKNOWN";

	static {
		try {
			Metric.IP = InetAddress.getLocalHost().getHostAddress().toString();
		} catch (UnknownHostException unused) {
			Metric.IP = "UNKNOWN";
		}

	}

	private void init() {
		this.stage = 0;
		this.timeStamp = 0;
	
		this.ip = Metric.IP;

		this.status = Status.RUN;

		this.readSucceedRecords = new AtomicLong(0);
		this.readSucceedBytes = new AtomicLong(0);
		this.readFailedRecords = new AtomicLong(0);
		this.readFailedBytes = new AtomicLong(0);

		this.writeReceivedRecords = new AtomicLong(0);
		this.writeReceivedBytes = new AtomicLong(0);
		this.writeFailedRecords = new AtomicLong(0);
		this.writeFailedBytes = new AtomicLong(0);

		this.message = new HashMap<String, List<String>>();
	}

	public Metric() {
		this.init();
	}

	public void reset() {
		this.init();
	}

	@Override
	public Metric clone() {
		Metric statistics = new Metric();

		try {
			BeanUtils.copyProperties(statistics, this);
		} catch (Exception e) {
			throw DataXException.asDataXException(FrameworkErrorCode.INNER_ERROR,
					"克隆metric出错.");
		}

		return statistics;
	}

	public long getReadSucceedRecords() {
		return readSucceedRecords.longValue();
	}

	public void setReadSucceedRecords(long readSucceedRecords) {
		this.readSucceedRecords.set(readSucceedRecords);
	}

	public long getReadSucceedBytes() {
		return readSucceedBytes.longValue();
	}

	public void setReadSucceedBytes(long readSucceedBytes) {
		this.readSucceedBytes.set(readSucceedBytes);
	}

	public long getReadFailedRecords() {
		return readFailedRecords.longValue();
	}

	public void setReadFailedRecords(long readFailedRecords) {
		this.readFailedRecords.set(readFailedRecords);
	}

	public long getReadFailedBytes() {
		return readFailedBytes.longValue();
	}

	public void setReadFailedBytes(long readFailedBytes) {
		this.readFailedBytes.set(readFailedBytes);
	}

	public long getWriteReceivedRecords() {
		return writeReceivedRecords.longValue();
	}

	public void setWriteReceivedRecords(long writeReceivedRecords) {
		this.writeReceivedRecords.set(writeReceivedRecords);
	}

	public long getWriteReceivedBytes() {
		return writeReceivedBytes.longValue();
	}

	public void setWriteReceivedBytes(long writeReceivedBytes) {
		this.writeReceivedBytes.set(writeReceivedBytes);
	}

	public long getWriteFailedRecords() {
		return writeFailedRecords.longValue();
	}

	public void setWriteFailedRecords(long writeFailedRecords) {
		this.writeFailedRecords.set(writeFailedRecords);
	}

	public long getWriteFailedBytes() {
		return writeFailedBytes.longValue();
	}

	public void setWriteFailedBytes(long writeFailedBytes) {
		this.writeFailedBytes.set(writeFailedBytes);
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public int getStage() {
		return stage;
	}

	public void setStage(int stage) {
		this.stage = stage;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String getErrorMessage() {
		return getError() == null ? "" : getError().getMessage();
	}

	public Throwable getError() {
		return throwable;
	}

	public void setError(Throwable exception) {
		this.throwable = exception;
	}

	public synchronized void addMessage(String key, String value) {
		if (null == this.message.get(key)) {
			this.message.put(key, new ArrayList<String>());
		}
		this.message.get(key).add(value);
	}

	public Map<String, List<String>> getMessage() {
		return this.message;
	}

	public List<String> getMessage(final String key) {
		return this.message.get(key);
	}

	public synchronized long getTotalReadRecords() {
		return this.readSucceedRecords.longValue()
				+ this.readFailedRecords.longValue();
	}

	public synchronized long getTotalReadBytes() {
		return this.readSucceedBytes.longValue()
				+ this.readFailedBytes.longValue();
	}

	public long getWriterReceivedRecords() {
		return this.writeReceivedRecords.longValue();
	}

	public long getWriterReceivedBytes() {
		return this.writeReceivedBytes.longValue();
	}

	public synchronized long getWriteSucceedRecords() {
		return this.writeReceivedRecords.longValue()
				- this.writeFailedRecords.longValue();
	}

	public synchronized long getWriteSucceedBytes() {
		return this.writeReceivedBytes.longValue()
				- this.writeFailedBytes.longValue();
	}

    public synchronized long getErrorRecords() {
        return this.getReadFailedRecords() + this.getWriteFailedRecords();
    }

	public void incrReadSucceedRecords(long delta) {
		this.readSucceedRecords.getAndAdd(delta);
	}

	public void incrReadSucceedBytes(long delta) {
		this.readSucceedBytes.getAndAdd(delta);
	}

	public void incrReadFailedRecords(long delta) {
		this.readFailedRecords.getAndAdd(delta);
	}

	public void incrReadFailedBytes(long delta) {
		this.readFailedBytes.getAndAdd(delta);
	}

	public void incrWriteReceivedRecords(long delta) {
		this.writeReceivedRecords.getAndAdd(delta);
	}

	public void incrWriteReceivedBytes(long delta) {
		this.writeReceivedBytes.getAndAdd(delta);
	}

	public void incrWriteFailedRecords(long delta) {
		this.writeFailedRecords.getAndAdd(delta);
	}

	public void incrWriteFailedBytes(long delta) {
		this.writeFailedBytes.getAndAdd(delta);
	}

	public long getByteSpeed() {
		return byteSpeed;
	}

	public void setByteSpeed(long byteSpeed) {
		this.byteSpeed = byteSpeed;
	}

	public long getRecordSpeed() {
		return recordSpeed;
	}

	public void setRecordSpeed(long recordSpeed) {
		this.recordSpeed = recordSpeed;
	}

	public double getPercentage() {
		return percentage;
	}

	public void setPercentage(double percentage) {
		this.percentage = percentage;
	}

	public synchronized Metric mergeFrom(Metric metric) {
		this.readSucceedBytes.addAndGet(metric.getReadSucceedBytes());
		this.readSucceedRecords.addAndGet(metric.getReadSucceedRecords());
		this.readFailedBytes.addAndGet(metric.getReadFailedBytes());
		this.readFailedRecords.addAndGet(metric.getReadFailedRecords());

		this.writeReceivedBytes.addAndGet(metric.getWriteReceivedBytes());
		this.writeReceivedRecords.addAndGet(metric.getWriteReceivedRecords());
		this.writeFailedBytes.addAndGet(metric.getWriteFailedBytes());
		this.writeFailedRecords.addAndGet(metric.getWriteFailedRecords());

		this.stage += metric.getStage();

        // merge status
        if(this.getStatus()==Status.FAIL || metric.getStatus()==Status.FAIL) {
            this.setStatus(Status.FAIL);
        } else if (this.getStatus()==Status.RUN || metric.getStatus()==Status.RUN) {
            this.setStatus(Status.RUN);
        }

		this.throwable = (this.throwable != null ? this.throwable : metric
				.getError());

		for (Entry<String, List<String>> entry : metric.getMessage().entrySet()) {
			String key = entry.getKey();
			List<String> valueList = this.message.get(key);
			if (valueList == null) {
				valueList = new ArrayList<String>();
				this.message.put(key, valueList);
			}
			valueList.addAll(entry.getValue());
		}

		return this;
	}
}
