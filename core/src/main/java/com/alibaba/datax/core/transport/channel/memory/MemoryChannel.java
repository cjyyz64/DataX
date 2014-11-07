package com.alibaba.datax.core.transport.channel.memory;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.FrameworkErrorCode;

/**
 * 
 */
public class MemoryChannel extends Channel {

	private int bufferSize = 0;

	private ArrayBlockingQueue<Record> queue = null;

	private ReentrantLock lock;

	private Condition notInsufficient, notEmpty;

	public MemoryChannel(final Configuration configuration) {
		super(configuration);
		this.queue = new ArrayBlockingQueue<Record>(this.getCapacity());
		this.bufferSize = configuration
				.getInt(CoreConstant.DATAX_CORE_TRANSPORT_EXCHANGER_BUFFERSIZE);

		lock = new ReentrantLock();
		notInsufficient = lock.newCondition();
		notEmpty = lock.newCondition();
	}

	@Override
	public void close() {
		super.close();
		try {
			this.queue.put(TerminateRecord.get());
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	protected void doPush(Record r) {
		try {
			this.queue.put(r);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	protected void doPushAll(Collection<Record> rs) {
		try {
			lock.lockInterruptibly();
			while (rs.size() > this.queue.remainingCapacity()) {
				notInsufficient.await(200L, TimeUnit.MILLISECONDS);
			}

			this.queue.addAll(rs);
			notEmpty.signalAll();
		} catch (InterruptedException e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	protected Record doPull() {
		try {
			return this.queue.take();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	@Override
	protected void doPullAll(Collection<Record> rs) {
		assert rs != null;
		rs.clear();
		try {
			lock.lockInterruptibly();
			while (this.queue.drainTo(rs, bufferSize) <= 0) {
				notEmpty.await(200L, TimeUnit.MILLISECONDS);
			}

			notInsufficient.signalAll();
		} catch (InterruptedException e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int size() {
		return this.queue.size();
	}

	@Override
	public boolean isEmpty() {
		return this.queue.isEmpty();
	}

}
