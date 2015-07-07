package com.alibaba.datax.test.simulator.util;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;

import java.util.List;

public class RecordReceiverForTest implements RecordReceiver {

	private List<Record> contents = null;

	public RecordReceiverForTest(List<Record> contents) {
		this.contents = contents;
	}

	@Override
	public Record getFromReader() {
        if(!contents.isEmpty()){
            return contents.remove(0);
        }
		return null;
	}

	@Override
	public void shutdown(){

	}
}
