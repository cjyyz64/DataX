package com.alibaba.datax.common.element;

import java.sql.Date;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.base.BaseTest;
import com.alibaba.datax.common.exception.DataXException;

public class NumberColumnTest extends BaseTest  {

	@Test
	public void test() {
		NumberColumn number = new NumberColumn(2.3d);
		Assert.assertTrue(number.asLong().equals(2L));
		Assert.assertTrue(number.asDouble().equals(2.3d));
		System.out.println(number.asString());
		Assert.assertTrue(number.asString().equals("2.3"));
		Assert.assertTrue(number.asBoolean().equals(true));
		try {
			Assert.assertTrue(number.asDate().equals(new Date(2L)));
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
		try {
			number.asBytes();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		number = new NumberColumn(2);
		Assert.assertTrue(number.asLong().equals(2L));
		Assert.assertTrue(number.asDouble().equals(2.0d));
		Assert.assertTrue(number.asString().equals("2"));
		Assert.assertTrue(number.asDate().equals(new Date(2L)));
		Assert.assertTrue(number.asBoolean().equals(true));
		try {
			number.asBytes();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		number = new NumberColumn(2L);
		Assert.assertTrue(number.asLong().equals(2L));
		Assert.assertTrue(number.asDouble().equals(2.0d));
		Assert.assertTrue(number.asString().equals("2"));
		Assert.assertTrue(number.asBoolean().equals(true));
		try {
			number.asBytes();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		number = new NumberColumn("2.3");
		Assert.assertTrue(number.asLong().equals(2L));
		Assert.assertTrue(number.asDouble().equals(2.3d));
		Assert.assertTrue(number.asString().equals("2.3"));
		Assert.assertTrue(number.asBoolean().equals(true));
		try {
			number.asBytes();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		number = new NumberColumn();
		Assert.assertTrue(number.asLong() == null);
		Assert.assertTrue(number.asDouble() == null);
		Assert.assertTrue(number.asString() == null);
		Assert.assertTrue(number.asDate() == null);
		Assert.assertTrue(number.asBoolean() == null);
		try {
			number.asBytes();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			number = new NumberColumn("bazhen");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

	}
}
