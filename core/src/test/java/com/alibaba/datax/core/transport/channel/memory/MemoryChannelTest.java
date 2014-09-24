package com.alibaba.datax.core.transport.channel.memory;

import com.alibaba.datax.common.element.NumberColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scaffold.ConfigurationProducer;
import com.alibaba.datax.core.scaffold.RecordProducer;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.datax.core.util.CoreConstant;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MemoryChannelTest extends CaseInitializer {

	// 测试SEQ
	@Test
	public void test_seq() {
		int capacity = 4;

		System.out.println(ConfigurationProducer.produce().toJSON());

        Configuration configuration = ConfigurationProducer.produce();
        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID, 0);
		Channel channel = new MemoryChannel(configuration);

		Record record = null;
		for (int i = 0; i < capacity; i++) {
			record = RecordProducer.produceRecord();
			record.setColumn(0, new NumberColumn(i));
			channel.push(record);
		}

		for (int i = 0; i < capacity; i++) {
			record = channel.pull();
			System.out.println(record.getColumn(0).asLong());
			Assert.assertTrue(record.getColumn(0).asLong() == i);
		}

		List<Record> records = new ArrayList<Record>(capacity);
		for (int i = 0; i < capacity; i++) {
			record = RecordProducer.produceRecord();
			record.setColumn(0, new NumberColumn(i));
			records.add(record);
		}
		channel.pushAll(records);

		channel.pullAll(records);
		System.out.println(records.size());
		for (int i = 0; i < capacity; i++) {
			System.out.println(records.get(i).getColumn(0).asLong());
			Assert.assertTrue(records.get(i).getColumn(0).asLong() == i);
		}
	}

	@Test
	public void test_Block() throws InterruptedException {
        Configuration configuration = ConfigurationProducer.produce();
        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID, 0);
		Channel channel = new MemoryChannel(configuration);

		int tryCount = 100;
		int capacity = ConfigurationProducer.produce().getInt(
				CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY);

		Thread thread = new Thread(new Consumer(channel, tryCount * capacity));
		thread.start();

		List<Record> records = new ArrayList<Record>(capacity);
		for (int i = 0; i < capacity; i++) {
			Record record = RecordProducer.produceRecord();
			record.setColumn(0, new NumberColumn(i));
			records.add(record);
		}

		for (int i = 0; i < tryCount; i++) {
			channel.pushAll(records);
		}

		channel.push(TerminateRecord.get());

		Thread.sleep(1000L);

		thread.join();

	}
}

class Consumer implements Runnable {

	private Channel channel = null;

	private int needCapacity = 0;

	public Consumer(Channel channel, int needCapacity) {
		this.channel = channel;
		this.needCapacity = needCapacity;
		return;
	}

	@Override
	public void run() {
		List<Record> records = new ArrayList<Record>();

		boolean isTermindate = false;
		int counter = 0;

		while (true) {
			this.channel.pullAll(records);
			for (final Record each : records) {
				if (each == TerminateRecord.get()) {
					isTermindate = true;
					break;
				}
				counter++;
				continue;
			}

			if (isTermindate) {
				break;
			}
		}

		System.out.println(String.format("Need %d, Get %d .", needCapacity,
				counter));
		Assert.assertTrue(counter == needCapacity);
	}

}
