package com.alibaba.datax.test.simulator.util;

import java.util.List;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.core.transport.record.TerminateRecord;

public class RecordReceiverForTest implements RecordReceiver {

	private List<Record> contents = null;
	private int index = 0;

	public RecordReceiverForTest(List<Record> contents) {
		this.contents = contents;
	}

	// TODO 考虑多个线程同时调用？
	@Override
	public Record getFromReader() {
		if (index < contents.size()) {
			Record r = contents.get(index);
			contents.remove(index);
			index++;
			return r;
		}
		return null;
	}
}
