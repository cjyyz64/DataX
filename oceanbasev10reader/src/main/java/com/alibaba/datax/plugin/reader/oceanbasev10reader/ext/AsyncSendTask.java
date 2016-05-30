package com.alibaba.datax.plugin.reader.oceanbasev10reader.ext;

import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.TaskContext;

/**
 * 异步发送线程
 * 
 * @author biliang.wbl
 *
 */
public class AsyncSendTask extends Thread {

	private static final Logger log = LoggerFactory.getLogger(AsyncSendTask.class);

	private LinkedBlockingQueue<Record> queue;

	private final RecordSender recordSender;
	private final TaskContext context;

	public AsyncSendTask(RecordSender recordSender, TaskContext context, int size) {
		this.recordSender = recordSender;
		assert recordSender != null;
		this.context = context;
		queue = new LinkedBlockingQueue<Record>(size);
	}

	@Override
	public void run() {
		while (true) {
			try {
				Record row = queue.take();
				if (row == NULL) {
					break;
				}
				assert row.getColumnNumber() >= context.getTransferColumnNumber();

				if (row.getColumnNumber() == context.getTransferColumnNumber()) {
					recordSender.sendToWriter(row);
				} else {
					Record newRow = recordSender.createRecord();
					for (int i = 0; i < context.getTransferColumnNumber(); i++) {
						newRow.addColumn(row.getColumn(i));
					}
					recordSender.sendToWriter(newRow);
				}
				context.setSavePoint(row);
			} catch (InterruptedException e) {
				log.warn("record send exception", e);
			}
		}
	}

	public void shutdown() {
		send(NULL);
	}

	public void send(Record row) {
		while (true) {
			try {
				queue.put(row);
				break;
			} catch (InterruptedException e) {
			}
		}
	}

	private final Record NULL = new Record() {
		@Override
		public void addColumn(Column arg0) {
		}

		@Override
		public int getByteSize() {
			return 0;
		}

		@Override
		public Column getColumn(int arg0) {
			return null;
		}

		@Override
		public int getColumnNumber() {
			return 0;
		}

		@Override
		public int getMemorySize() {
			return 0;
		}

		@Override
		public void setColumn(int arg0, Column arg1) {
		}
	};
}
