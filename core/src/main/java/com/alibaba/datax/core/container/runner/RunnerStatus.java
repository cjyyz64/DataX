package com.alibaba.datax.core.container.runner;

import com.alibaba.datax.core.util.Status;

/**
 * Created by jingxing on 14-9-1.
 */
public class RunnerStatus {

    private int status;

    private Exception exception;

    public RunnerStatus() {
        this(Status.RUN.value(), null);
    }

    public RunnerStatus(int status, final Exception exception) {
        setStatus(status);
        setException(exception);
    }

    public boolean isSuccess() {
        return status == Status.SUCCESS.value();
    }

    public boolean isFail() {
        return status == Status.FAIL.value();
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(final Exception exception) {
        this.exception = exception;
    }

}
