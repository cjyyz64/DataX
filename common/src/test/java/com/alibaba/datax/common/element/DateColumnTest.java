package com.alibaba.datax.common.element;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.base.BaseTest;
import com.alibaba.datax.common.exception.DataXException;

public class DateColumnTest extends BaseTest  {
	@Test
	public void test() {
		long time = System.currentTimeMillis();
		DateColumn date = new DateColumn(time);
		Assert.assertTrue(date.getType().equals(Column.Type.DATE));
		Assert.assertTrue(date.asDate().getTime() == time);
		Assert.assertTrue(date.asLong().equals(time));
		System.out.println(date.asString());
		Assert.assertTrue(date.asString().startsWith("201"));

		try {
			date.asBytes();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			date.asDouble();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
	}

	@Test
	public void test_null() {
		DateColumn date = new DateColumn();
		Assert.assertTrue(date.getType().equals(Column.Type.DATE));

		Assert.assertTrue(date.asDate() == null);
		Assert.assertTrue(date.asLong() == null);

		try {
			date.asBytes();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			date.asDouble();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			date.asBoolean();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

	}
}
