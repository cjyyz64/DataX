package com.alibaba.datax.core.util;

public final class SleepQuiet {
	public static void sleep(long millsec) {
		try {
			Thread.sleep(millsec);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}
