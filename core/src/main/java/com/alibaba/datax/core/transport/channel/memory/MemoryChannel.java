package com.alibaba.datax.core.transport.channel.memory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 内存Channel的具体实现，底层其实是一个ArrayBlockingQueue
 *
 */
public class MemoryChannel extends Channel {

	private int bufferSize = 0;

	private AtomicInteger memoryBytes = new AtomicInteger(0);

	private ArrayBlockingQueue<Record> queue = null;

	private ReentrantLock lock;

	private Condition notInsufficient, notEmpty;

	public MemoryChannel(final Configuration configuration) {
		super(configuration);
		this.queue = new ArrayBlockingQueue<Record>(this.getCapacity());
		this.bufferSize = configuration.getInt(CoreConstant.DATAX_CORE_TRANSPORT_EXCHANGER_BUFFERSIZE);

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
	public void clear(){
		this.queue.clear();
	}

	@Override
	protected void doPush(Record r) {
		try {
            //首先判断下队列的空间，如果full，则统计一次waitWriter。因为后面的offer可能统计不到，比如200ms内，writer消费掉了数据。但实际上，还是writer阻塞了整体速度。
            if (this.queue.remainingCapacity() < 1) {
                waitWriterCount++;
            }
            //对于200ms内，还没有消费掉，再增加counter
            //非线程模型，因此while无退不出来的风险
            while (!this.queue.offer(r, 200L, TimeUnit.MILLISECONDS)) {
                waitWriterCount++;
            }
            memoryBytes.addAndGet(r.getByteSize());
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	protected void doPushAll(Collection<Record> rs) {
		try {
			lock.lockInterruptibly();
			int bytes = getRecordBytes(rs);
			while (memoryBytes.get() + bytes > this.byteCapacity || rs.size() > this.queue.remainingCapacity()) {
				notInsufficient.await(200L, TimeUnit.MILLISECONDS);
                waitWriterCount++;
            }

			this.queue.addAll(rs);
			memoryBytes.addAndGet(bytes);
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
            //首先判断下队列的空间，如果full，则统计一次waitWriter。因为后面的poll可能统计不到，比如200ms内，Reader来了数据。但实际上，还是reader阻塞了整体速度。
            if (this.queue.isEmpty()) {
                waitReaderCount++;
            }
            //对于200ms内，还没有数据，再增加counter
            //非线程模型，因此while无退不出来的风险
            Record r = this.queue.poll(200L, TimeUnit.MILLISECONDS);
            while (r == null) {
                r = this.queue.poll(200L, TimeUnit.MILLISECONDS);
                waitReaderCount++;
            }
            memoryBytes.addAndGet(-r.getByteSize());
			return r;
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
                waitReaderCount++;
			}
			int bytes = getRecordBytes(rs);
			memoryBytes.addAndGet(-bytes);
			notInsufficient.signalAll();
		} catch (InterruptedException e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			lock.unlock();
		}
	}

	private int getRecordBytes(Collection<Record> rs){
		int bytes = 0;
		for(Record r : rs){
			bytes += r.getByteSize();
		}
		return bytes;
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
