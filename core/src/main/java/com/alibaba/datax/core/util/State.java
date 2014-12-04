package com.alibaba.datax.core.util;

/**
 * Created by jingxing on 14-8-27.
 *
 */
public enum State {
    FAIL(-1), SUCCESS(0), RUN(1), RETRY(2);

    private int state;

    public int value() {
        return state;
    }

    private State(int state) {
        this.state = state;
    }

    public static String toString(int state) {
        switch (state) {
            case -1:
                return "FAIL";
            case 0:
                return "SUCCESS";
            case 1:
                return "RUN";
            case 2:
                return "RETRY";
            default:
                return "UNKNOWN";
        }
    }

    public boolean isFailed() {
        return this.state == FAIL.value();
    }

    public boolean isRunning() {
        return this.state == RUN.value();
    }

    public boolean isSucceed() {
        return this.state == SUCCESS.value();
    }
}
