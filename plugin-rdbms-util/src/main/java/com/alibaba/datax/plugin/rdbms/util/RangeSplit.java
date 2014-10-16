package com.alibaba.datax.plugin.rdbms.util;

import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * 主要提供对范围的解析
 */
public class RangeSplit {

	public static List<String> splitStringRange(final String begin,
			final String end, int count) {
		if (count <= 0) {
			throw new IllegalArgumentException("count must larger than 0 .");
		}

		if (begin.compareTo(end) > 0) {
			throw new IllegalArgumentException(String.format(
					"Begin [%s] must smaller than End [%s] .",
					begin.toString(), end.toString()));
		}

		List<String> result = new ArrayList<String>();
		if (1 == count || begin.equals(end)) {
			result.add(begin);
			result.add(end);
			return result;
		}

		BigInteger left = RangeSplit
				.toNumber(RangeSplit.adjust2Max(begin, end));
		BigInteger right = RangeSplit.toNumber(RangeSplit
				.adjust2Max(end, begin));

		BigInteger step = RangeSplit.step(left, right, count);
		List<BigInteger> ranges = RangeSplit.toSlice(left, right, step, count);

		for (BigInteger each : ranges) {
			result.add(toString(each));
		}

		return result;
	}

	public static List<BigInteger> splitBigIntegerRange(final BigInteger begin,
			final BigInteger end, int count) {
		if (count <= 0) {
			throw new IllegalArgumentException("count must larger than 0 .");
		}

		if (begin.compareTo(end) > 0) {
			throw new IllegalArgumentException(String.format(
					"Begin [%s] must smaller than End [%s] .",
					begin.toString(), end.toString()));
		}

		List<BigInteger> result = new ArrayList<BigInteger>();
		if (1 == count || begin.compareTo(end) == 0) {
			result.add(begin);
			result.add(end);
			return result;
		}

		BigInteger step = RangeSplit.step(begin, end, count);
		return RangeSplit.toSlice(begin, end, step, count);
	}

	public static List<Long> splitLongRange(long begin, long end, int count) {
		return toLongs(RangeSplit.splitBigIntegerRange(
				BigInteger.valueOf(begin), BigInteger.valueOf(end), count));
	}

	protected static List<Long> toLongs(final List<BigInteger> integers) {
		List<Long> longs = new ArrayList<Long>();
		for (final BigInteger each : integers) {
			longs.add(each.longValue());
		}
		return longs;
	}

	protected static List<BigInteger> toSlice(final BigInteger begin,
			final BigInteger end, final BigInteger step, int count) {
		List<BigInteger> result = new ArrayList<BigInteger>();

		for (BigInteger start = new BigInteger(begin.toString()); start
				.compareTo(end) <= 0 && count-- >= 0; start = start.add(step)) {
			result.add(start);
		}

		result.set(result.size() - 1, end);
		return result;
	}

	protected static BigInteger step(final BigInteger begin,
			final BigInteger end, int count) {
		BigInteger step = end.subtract(begin).divide(BigInteger.valueOf(count));
		if (step.compareTo(BigInteger.valueOf(0)) > 0) {
			return step;
		}

		return step.add(BigInteger.valueOf(1L));
	}

	protected static String toString(BigInteger integer) {

		StringBuilder result = new StringBuilder();

		BigInteger base = BigInteger.valueOf(128L);

		while (integer.compareTo(base) >= 0) {
			result.append((char) integer.mod(base).intValue());
			integer = integer.divide(base);
		}
		result.append((char) integer.intValue());

		return result.reverse().toString();
	}

	protected static char[] adjust2Max(final String left, final String right) {
		int size = Math.max(left.length(), right.length());

		char[] result = new char[size];
		for (int i = 0; i < left.length(); i++) {
			result[i] = left.charAt(i);
		}

		if (left.length() == size) {
			return result;
		}

		for (int i = left.length(); i < result.length; i++) {
			result[i] = '\0';
		}

		return result;
	}

	protected static BigInteger toNumber(final char[] chars) {
		assert chars != null;

		BigInteger base = BigInteger.valueOf(128L);
		BigInteger result = BigInteger.valueOf(0L);

		for (Character c : chars) {
			BigInteger adder = NumberUtils.createBigInteger(String
					.valueOf((int) c));
			result = result.multiply(base).add(adder);
		}

		return result;
	}
}
