package com.alibaba.datax.plugin.rdbms.util;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

public class RangeSplitTest {

	private static <T> boolean isEqual(final List<BigInteger> left,
			final List<T> right) {
		if (null == left || null == right) {
			return false;
		}

		if (left.size() != right.size()) {
			return false;
		}

		for (int i = 0; i < left.size(); i++) {
			BigInteger lefter = left.get(i);
			Object righer = right.get(i);
			if (null == lefter || null == righer) {
				return false;
			}
			if (!lefter.toString().equals(righer.toString())) {
				return false;
			}
		}

		return true;
	}

	@Test
	public void test_splitStringRange() {
		List<String> result = null;

		result = RangeSplit.splitStringRange("A", "A", 1);
		Assert.assertTrue(result.size() == 2);
		Assert.assertTrue(result.get(0).equals("A"));
		Assert.assertTrue(result.get(1).equals("A"));

		result = RangeSplit.splitStringRange("A", "B", 1);
		Assert.assertTrue(result.size() == 2);
		Assert.assertTrue(result.get(0).equals("A"));
		Assert.assertTrue(result.get(1).equals("B"));

		result = RangeSplit.splitStringRange("A", "Z", 1);
		Assert.assertTrue(result.size() == 2);
		Assert.assertTrue(result.get(0).equals("A"));
		Assert.assertTrue(result.get(1).equals("Z"));

		result = RangeSplit.splitStringRange("A", "A", 100);
		Assert.assertTrue(result.size() == 2);
		Assert.assertTrue(result.get(0).equals("A"));
		Assert.assertTrue(result.get(1).equals("A"));

		result = RangeSplit.splitStringRange("A", "B", 100);
		Assert.assertTrue(result.size() == 2);
		Assert.assertTrue(result.get(0).equals("A"));
		Assert.assertTrue(result.get(1).equals("B"));

		result = RangeSplit.splitStringRange("A", "C", 2);
		Assert.assertTrue(result.size() == 3);
		Assert.assertTrue(result.get(0).equals("A"));
		Assert.assertTrue(result.get(1).equals("B"));
		Assert.assertTrue(result.get(2).equals("C"));

		result = RangeSplit.splitStringRange("A", "D", 3);
		Assert.assertTrue(result.size() == 4);
		Assert.assertTrue(result.get(0).equals("A"));
		Assert.assertTrue(result.get(1).equals("B"));
		Assert.assertTrue(result.get(2).equals("C"));
		Assert.assertTrue(result.get(3).equals("D"));

		result = RangeSplit.splitStringRange("A", "Z", 3);
		Assert.assertTrue(result.size() == 4);
		System.out.println(result);
		Assert.assertTrue(result.get(0).equals("A"));
		Assert.assertTrue(result.get(1).equals("I"));
		Assert.assertTrue(result.get(2).equals("Q"));
		Assert.assertTrue(result.get(3).equals("Z"));

		result = RangeSplit.splitStringRange("AA", "AZ", 3);
		Assert.assertTrue(result.size() == 4);
		System.out.println(result);
		Assert.assertTrue(result.get(0).equals("AA"));
		Assert.assertTrue(result.get(1).equals("AI"));
		Assert.assertTrue(result.get(2).equals("AQ"));
		Assert.assertTrue(result.get(3).equals("AZ"));

		result = RangeSplit.splitStringRange("AAAAAA", "ZZ", 3);
		System.out.println(result);
		Collections.sort(result);
		CollectionUtils.isEqualCollection(result,
				RangeSplit.splitStringRange("AAAAAA", "ZZ", 3));
		
		result = RangeSplit.splitStringRange("A", "ZZZZZZZZZZZZZZZZZZZZZZZZ", 100);
		System.out.println(result);
		Collections.sort(result);
		CollectionUtils.isEqualCollection(result,
				RangeSplit.splitStringRange("A", "ZZZZZZZZZZZZZZZZZZZZZZZZ", 100));
	}

	@Test
	public void test_splitBigIntegerRange() {
		List<BigInteger> result = null;

		result = RangeSplit.splitBigIntegerRange(BigInteger.valueOf(0L),
				BigInteger.valueOf(0L), 1);
		Assert.assertTrue(result.size() == 2);
		Assert.assertTrue(result.get(0).intValue() == 0);
		Assert.assertTrue(result.get(1).intValue() == 0);

		result = RangeSplit.splitBigIntegerRange(BigInteger.valueOf(0L),
				BigInteger.valueOf(1L), 1);
		Assert.assertTrue(result.size() == 2);
		Assert.assertTrue(result.get(0).intValue() == 0);
		Assert.assertTrue(result.get(1).intValue() == 1);

		result = RangeSplit.splitBigIntegerRange(
				BigInteger.valueOf(Long.MIN_VALUE),
				BigInteger.valueOf(Long.MAX_VALUE), 1);
		Assert.assertTrue(result.size() == 2);
		Assert.assertTrue(result.get(0).longValue() == Long.MIN_VALUE);
		Assert.assertTrue(result.get(1).longValue() == Long.MAX_VALUE);

		try {
			result = RangeSplit.splitBigIntegerRange(
					BigInteger.valueOf(Long.MAX_VALUE),
					BigInteger.valueOf(Long.MIN_VALUE), 1);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		result = RangeSplit.splitBigIntegerRange(BigInteger.valueOf(0L),
				BigInteger.valueOf(4L), 3);
		System.out.println(result);
		List<Long> compare = new ArrayList<Long>();
		compare.add(0L);
		compare.add(1L);
		compare.add(2L);
		compare.add(4L);
		Assert.assertTrue(isEqual(result, compare));

		result = RangeSplit.splitBigIntegerRange(BigInteger.valueOf(1L),
				BigInteger.valueOf(8L), 3);
		System.out.println(result);
		compare.clear();
		compare.add(1L);
		compare.add(3L);
		compare.add(5L);
		compare.add(8L);
		Assert.assertTrue(isEqual(result, compare));

		result = RangeSplit.splitBigIntegerRange(BigInteger.valueOf(1L),
				BigInteger.valueOf(2L), 4);
		System.out.println(result);
		compare.clear();
		compare.add(1L);
		compare.add(2L);
		Assert.assertTrue(isEqual(result, compare));

		result = RangeSplit.splitBigIntegerRange(BigInteger.valueOf(-1L),
				BigInteger.valueOf(1L), 4);
		System.out.println(result);
		compare.clear();
		compare.add(-1L);
		compare.add(0L);
		compare.add(1L);

		Assert.assertTrue(isEqual(result, compare));
	}

}
