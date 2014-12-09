package com.alibaba.datax.core.util;

public final class SleepQuiet {
	public static void sleep(long millsec) {
		try {
			Thread.sleep(millsec);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

    public static void sleepAndSkipInterrupt(long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            System.err.println();
            System.err.println("sleep end, message: " + e.getMessage());
        }
    }
}
