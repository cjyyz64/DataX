package com.alibaba.datax.core.scheduler.standalone;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.container.AbstractContainer;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.Status;

/**
 * Created by jingxing on 14-8-28.
 */
public class SlaveContainerRunner implements Runnable {

	private AbstractContainer slave;

	private Status status;

	public SlaveContainerRunner(AbstractContainer slave) {
		this.slave = slave;
		this.status = Status.SUCCESS;
	}

	@Override
	public void run() {
		try {
			this.slave.start();
			this.status = Status.SUCCESS;
		} catch (Throwable e) {
			this.status = Status.FAIL;
			throw DataXException.asDataXException(
					FrameworkErrorCode.INNER_ERROR, e);
		}
	}

	public AbstractContainer getSlave() {
		return slave;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}
}
