package com.alibaba.datax.core.util;

/**
 * Created by jingxing on 14-8-27.
 *
 */
public enum Status {
    FAIL(-1), SUCCESS(0), RUN(1), RETRY(2);

    private int status;

    public int value() {
        return status;
    }

    private Status(int status) {
        this.status = status;
    }

    public static String toString(int status) {
        switch (status) {
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
}
