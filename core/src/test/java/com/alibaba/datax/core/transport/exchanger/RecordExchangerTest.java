package com.alibaba.datax.core.transport.exchanger;

import com.alibaba.datax.core.statistics.communication.Communication;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scaffold.ConfigurationProducer;
import com.alibaba.datax.core.scaffold.RecordProducer;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.channel.memory.MemoryChannel;
import com.alibaba.datax.core.util.CoreConstant;

public class RecordExchangerTest extends CaseInitializer {

	private Configuration configuration = null;

	@Before
	public void before() {
		this.configuration = ConfigurationProducer.produce();
		this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID, 1);
		return;
	}

	@Test
	public void test_Exchanger() {
		Channel channel = new MemoryChannel(configuration);
        channel.setCommunication(new Communication());

		int capacity = 10;
		Record record = null;
		RecordExchanger recordExchanger = new RecordExchanger(channel);

		for (int i = 0; i < capacity; i++) {
			record = RecordProducer.produceRecord();
			record.setColumn(0, new LongColumn(i));
			recordExchanger.sendToWriter(record);
		}

		channel.close();

		int counter = 0;
		while ((record = recordExchanger.getFromReader()) != null) {
			System.out.println(record.getColumn(0).toString());
			Assert.assertTrue(record.getColumn(0).asLong() == counter);
			counter++;
		}

		Assert.assertTrue(capacity == counter);
	}

	@Test
	public void test_BufferExchanger() {
		Channel channel = new MemoryChannel(configuration);
        channel.setCommunication(new Communication());

		int capacity = 10;
		Record record = null;
		BufferedRecordExchanger recordExchanger = new BufferedRecordExchanger(
				channel);

		for (int i = 0; i < capacity; i++) {
			record = RecordProducer.produceRecord();
			record.setColumn(0, new LongColumn(i));
			recordExchanger.sendToWriter(record);
		}

		recordExchanger.flush();

		channel.close();

		int counter = 0;
		while ((record = recordExchanger.getFromReader()) != null) {
			System.out.println(record.getColumn(0).toString());
			Assert.assertTrue(record.getColumn(0).asLong() == counter);
			counter++;
		}

		System.out.println(String.format("Capacity: %d Counter: %d .",
				capacity, counter));
		Assert.assertTrue(capacity == counter);
	}
}
