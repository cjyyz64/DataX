package com.alibaba.datax.core.scheduler.standalone;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.container.TaskGroupContainer;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.State;

/**
 * Created by jingxing on 14-8-28.
 */
public class TaskGroupContainerRunner implements Runnable {

	private TaskGroupContainer taskGroupContainer;

	private State state;

	public TaskGroupContainerRunner(TaskGroupContainer taskGroup) {
		this.taskGroupContainer = taskGroup;
		this.state = State.SUCCESS;
	}

	@Override
	public void run() {
		try {
            Thread.currentThread().setName(
                    String.format("taskGroup-%d", this.taskGroupContainer.getTaskGroupId()));
            this.taskGroupContainer.start();
			this.state = State.SUCCESS;
		} catch (Throwable e) {
			this.state = State.FAIL;
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		}
	}

	public TaskGroupContainer getTaskGroupContainer() {
		return taskGroupContainer;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}
}
