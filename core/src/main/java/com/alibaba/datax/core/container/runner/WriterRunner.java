package com.alibaba.datax.core.container.runner;

import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public WriterRunner(AbstractTaskPlugin abstractTaskPlugin) {
		super(abstractTaskPlugin);
	}

	@Override
	public void run() {
		assert null != this.recordReceiver;

		Writer.Task taskWriter = (Writer.Task) this.getPlugin();
		try {
			LOG.debug("task writer starts to do init ...");
			taskWriter.init();
			LOG.debug("task writer starts to do prepare ...");
			taskWriter.prepare();
			LOG.debug("task writer starts to write ...");
			taskWriter.startWrite(recordReceiver);
			LOG.debug("task writer starts to do post ...");
			taskWriter.post();
			super.markSuccess();
		} catch (Throwable e) {
			LOG.error("Writer Runner Received Exceptions:", e);
			super.markFail(e);
		} finally {
			LOG.debug("task writer starts to do destroy ...");
			super.destroy();
		}
	}
}
