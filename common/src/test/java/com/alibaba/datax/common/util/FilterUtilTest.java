package com.alibaba.datax.common.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class FilterUtilTest {
	private static List<String> ALL_STRS;

	@BeforeClass
	public static void beforeClass() {
		ALL_STRS = new ArrayList<String>();
		ALL_STRS.add("pt=1/ds=hangzhou");
		ALL_STRS.add("pt=1/ds=shanghai");
		ALL_STRS.add("pt=2/ds2=hangzhou");
	}

	@Test
	public void test00() {
		String regular = "pt=[1|2]/ds=*";

		List<String> matched = FilterUtil.filterByRegular(ALL_STRS, regular);

		System.out.println("matched:" + matched);
		List<String> expected = new ArrayList<String>();
		expected.add(ALL_STRS.get(0));
		expected.add(ALL_STRS.get(1));

		Assert.assertEquals(expected.size(), matched.size());

		Collections.sort(expected);
		Collections.sort(matched);
		Assert.assertArrayEquals(expected.toArray(), matched.toArray());
	}

	@Test
	public void test01() {
		String regular = "pt=[1|2]/ds=.*";

		List<String> matched = FilterUtil.filterByRegular(ALL_STRS, regular);

		System.out.println("matched:" + matched);
		List<String> expected = new ArrayList<String>();
		expected.add(ALL_STRS.get(0));
		expected.add(ALL_STRS.get(1));

		Assert.assertEquals(expected.size(), matched.size());

		Collections.sort(expected);
		Collections.sort(matched);
		Assert.assertArrayEquals(expected.toArray(), matched.toArray());
	}

	@Test
	public void test02() {
		String regular = "pt=[1|2]/ds=.*";

		List<String> matched = FilterUtil.filterByRegular(ALL_STRS, regular);

		System.out.println("matched:" + matched);
		List<String> expected = new ArrayList<String>();
		expected.add(ALL_STRS.get(0));
		expected.add(ALL_STRS.get(1));

		Assert.assertEquals(expected.size(), matched.size());

		Collections.sort(expected);
		Collections.sort(matched);
		Assert.assertArrayEquals(expected.toArray(), matched.toArray());
	}

	@Test
	public void test03() {
		String regular = "pt=*";

		List<String> matched = FilterUtil.filterByRegular(ALL_STRS, regular);

		System.out.println("matched:" + matched);
		List<String> expected = new ArrayList<String>(ALL_STRS);

		Assert.assertEquals(expected.size(), matched.size());

		Collections.sort(expected);
		Collections.sort(matched);
		Assert.assertArrayEquals(expected.toArray(), matched.toArray());
	}

	@Test
	public void test04() {
		String regular = "^pt=*";

		List<String> matched = FilterUtil.filterByRegular(ALL_STRS, regular);

		System.out.println("matched:" + matched);
		List<String> expected = new ArrayList<String>(ALL_STRS);

		Assert.assertEquals(expected.size(), matched.size());

		Collections.sort(expected);
		Collections.sort(matched);
		Assert.assertArrayEquals(expected.toArray(), matched.toArray());
	}

	@Test
	public void test05() {
		String regular = "pt=1/ds=s[a-z]*";

		List<String> matched = FilterUtil.filterByRegular(ALL_STRS, regular);

		System.out.println("matched:" + matched);
		List<String> expected = new ArrayList<String>();
		expected.add(ALL_STRS.get(1));

		Assert.assertEquals(expected.size(), matched.size());

		Collections.sort(expected);
		Collections.sort(matched);
		Assert.assertArrayEquals(expected.toArray(), matched.toArray());
	}
}
