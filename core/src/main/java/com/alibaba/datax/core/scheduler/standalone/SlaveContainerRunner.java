package com.alibaba.datax.core.scheduler.standalone;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.container.SlaveContainer;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.Status;

/**
 * Created by jingxing on 14-8-28.
 */
public class SlaveContainerRunner implements Runnable {

	private SlaveContainer slave;

	private Status status;

	public SlaveContainerRunner(SlaveContainer slave) {
		this.slave = slave;
		this.status = Status.SUCCESS;
	}

	@Override
	public void run() {
		try {
            Thread.currentThread().setName(
                    String.format("slave-%d", this.slave.getSlaveId()));
            this.slave.start();
			this.status = Status.SUCCESS;
		} catch (Throwable e) {
			this.status = Status.FAIL;
			throw DataXException.asDataXException(
					FrameworkErrorCode.INNER_ERROR, e);
		}
	}

	public SlaveContainer getSlave() {
		return slave;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}
}
