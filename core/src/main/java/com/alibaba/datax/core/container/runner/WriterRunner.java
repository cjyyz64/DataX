package com.alibaba.datax.core.container.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.plugin.AbstractSlavePlugin;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;

/**
 * Created by jingxing on 14-9-1.
 * 
 * 单个slice的writer执行调用
 */
public class WriterRunner extends AbstractRunner implements Runnable {

	private static final Logger LOG = LoggerFactory
			.getLogger(WriterRunner.class);

	private RecordReceiver recordReceiver;

	public void setRecordReceiver(RecordReceiver receiver) {
		this.recordReceiver = receiver;
	}

	public WriterRunner(AbstractSlavePlugin abstractSlavePlugin) {
		super(abstractSlavePlugin);
	}

	@Override
	public void run() {
		assert null != this.recordReceiver;

		Writer.Slave writerSlave = (Writer.Slave) this.getPlugin();
		try {
			LOG.debug("slave writer starts to do init ...");
			writerSlave.init();
			LOG.debug("slave writer starts to do prepare ...");
			writerSlave.prepare();
			LOG.debug("slave writer starts to write ...");
			writerSlave.startWrite(recordReceiver);
			LOG.debug("slave writer starts to do post ...");
			writerSlave.post();
			super.markSuccess();
		} catch (Throwable e) {
			LOG.error("slave Writer Received Exceptions:", e);
			super.markFail(e);
		} finally {
			LOG.debug("slave writer starts to do destroy ...");
			super.destroy();
		}
	}
}
