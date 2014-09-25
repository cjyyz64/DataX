package com.alibaba.datax.core.container.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.plugin.AbstractSlavePlugin;
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

	public ReaderRunner(AbstractSlavePlugin abstractSlavePlugin) {
		super(abstractSlavePlugin);
	}

	@Override
	public void run() {
		assert null != this.recordSender;

		Reader.Slave readerSlave = (Reader.Slave) this.getPlugin();

		try {
			LOG.debug("slave reader starts to do init ...");
			readerSlave.init();
			LOG.debug("slave reader starts to do prepare ...");
			readerSlave.prepare();
			LOG.debug("slave reader starts to read ...");
			readerSlave.startRead(recordSender);
			recordSender.sendToWriter(TerminateRecord.get());
			recordSender.flush();
			LOG.debug("slave reader starts to do post ...");
			readerSlave.post();
			// automatic flush
			super.markSuccess();
		} catch (Throwable e) {
			LOG.error("Reader runner Received Exceptions:", e);
			super.markFail(e);
		} finally {
			LOG.debug("slave reader starts to do destroy ...");
			super.destroy();
		}
	}
}
