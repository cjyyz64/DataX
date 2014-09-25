package com.alibaba.datax.core.transport.exchanger;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.Validate;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.core.util.CoreConstant;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.record.TerminateRecord;

public class BufferedRecordExchanger implements RecordSender, RecordReceiver {

	private Channel channel;

	private Configuration configuration;

	private List<Record> buffer;

	private int bufferSize = 0;

	private int bufferIndex = 0;

	private static Class<? extends Record> RECORD_CLASS;

	@SuppressWarnings("unchecked")
	public BufferedRecordExchanger(final Channel channel) {
		assert null != channel;
		assert null != channel.getConfiguration();

		this.channel = channel;
		this.configuration = channel.getConfiguration();

		this.bufferSize = configuration
				.getInt(CoreConstant.DATAX_CORE_TRANSPORT_EXCHANGER_BUFFERSIZE);
		this.buffer = new ArrayList<Record>(bufferSize);

		try {
			BufferedRecordExchanger.RECORD_CLASS = ((Class<? extends Record>) Class
					.forName(configuration
							.getString(
									CoreConstant.DATAX_CORE_TRANSPORT_RECORD_CLASS,
									"com.alibaba.datax.core.transport.record.DefaultRecord")));
		} catch (Exception e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.CONFIG_ERROR, e);
		}
	}

	@Override
	public Record createRecord() {
		try {
			return BufferedRecordExchanger.RECORD_CLASS.newInstance();
		} catch (Exception e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.CONFIG_ERROR, e);
		}
	}

	@Override
	public void sendToWriter(Record record) {
		Validate.notNull(record, "record can not be null.");

		boolean isFull = (this.bufferIndex >= this.bufferSize);
		if (isFull) {
			flush();
		}

		this.buffer.add(record);
		this.bufferIndex++;
	}

	@Override
	public void flush() {
		this.channel.pushAll(this.buffer);
		this.buffer.clear();
		this.bufferIndex = 0;
	}

	@Override
	public Record getFromReader() {
		boolean isEmpty = (this.bufferIndex >= this.buffer.size());
		if (isEmpty) {
			receive();
		}

		Record record = this.buffer.get(this.bufferIndex++);
		if (record instanceof TerminateRecord) {
			this.channel.decreaseTerminateRecordMetric();
			record = null;
		}
		return record;
	}

	private void receive() {
		this.channel.pullAll(this.buffer);
		this.bufferIndex = 0;
		this.bufferSize = this.buffer.size();
	}
}
