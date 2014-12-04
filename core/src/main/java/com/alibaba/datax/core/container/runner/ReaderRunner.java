package com.alibaba.datax.core.container.runner;

import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.core.transport.record.TerminateRecord;

/**
 * Created by jingxing on 14-9-1.
 * 
 * 单个slice的reader执行调用
 */
public class ReaderRunner extends AbstractRunner implements Runnable {

	private static final Logger LOG = LoggerFactory
			.getLogger(ReaderRunner.class);

	private RecordSender recordSender;

	public void setRecordSender(RecordSender recordSender) {
		this.recordSender = recordSender;
	}

	public ReaderRunner(AbstractTaskPlugin abstractTaskPlugin) {
		super(abstractTaskPlugin);
	}

	@Override
	public void run() {
		assert null != this.recordSender;

		Reader.Task taskReader = (Reader.Task) this.getPlugin();

		try {
			LOG.debug("task reader starts to do init ...");
			taskReader.init();
			LOG.debug("task reader starts to do prepare ...");
			taskReader.prepare();
			LOG.debug("task reader starts to read ...");
			taskReader.startRead(recordSender);
			recordSender.sendToWriter(TerminateRecord.get());
			recordSender.flush();
			LOG.debug("task reader starts to do post ...");
			taskReader.post();
			// automatic flush
			super.markSuccess();
		} catch (Throwable e) {
			LOG.error("Reader runner Received Exceptions:", e);
			super.markFail(e);
		} finally {
			LOG.debug("task reader starts to do destroy ...");
			super.destroy();
		}
	}
}
