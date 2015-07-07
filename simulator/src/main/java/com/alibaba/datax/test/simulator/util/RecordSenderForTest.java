package com.alibaba.datax.test.simulator.util;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.core.transport.record.TerminateRecord;

import java.io.PrintWriter;
import java.util.List;

public class RecordSenderForTest implements RecordSender {
	private PrintWriter printWriter;

	private List<Record> noteRecordForTest;

	public RecordSenderForTest(PrintWriter printWriter,
			List<Record> noteRecordForTest) {

		this.printWriter = printWriter;
		this.noteRecordForTest = noteRecordForTest;
	}

	@Override
	public Record createRecord() {
		return new DefaultRecord();
	}

	@Override
	public void sendToWriter(Record record) {
		if (record instanceof TerminateRecord) {
			if (null != this.printWriter) {
				this.printWriter.close();
			}
		} else {
			if (null != this.printWriter) {
				this.printWriter.write(record.toString() + "\n");
				this.printWriter.flush();
			}
			if (null != this.noteRecordForTest) {
				this.noteRecordForTest.add(record);
			}
		}
	}

	@Override
	public void flush() {
		if (null != this.printWriter) {
			this.printWriter.flush();
		}
	}

	@Override
	public void terminate() {
		this.printWriter.write(TerminateRecord.get().toString() + "\n");
	}

	@Override
	public void shutdown(){

	}

}
