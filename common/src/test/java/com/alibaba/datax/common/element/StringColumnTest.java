package com.alibaba.datax.common.element;

import com.alibaba.datax.common.base.BaseTest;
import com.alibaba.datax.common.exception.DataXException;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

public class StringColumnTest extends BaseTest {

	@Test
	public void test_double() {
		DoubleColumn real = new DoubleColumn("3.14");
		Assert.assertTrue(real.asString().equals("3.14"));
		Assert.assertTrue(real.asDouble().equals(3.14d));
		Assert.assertTrue(real.asLong().equals(3L));

		try {
			real.asBoolean();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			real.asDate();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
	}

	@Test
	public void test_int() {
		LongColumn integer = new LongColumn("3");
		Assert.assertTrue(integer.asString().equals("3"));
		Assert.assertTrue(integer.asDouble().equals(3.0d));
		Assert.assertTrue(integer.asBoolean().equals(true));
		Assert.assertTrue(integer.asLong().equals(3L));
		System.out.println(integer.asDate());
	}

	@Test
	public void test_string() {
		StringColumn string = new StringColumn("bazhen");
		Assert.assertTrue(string.asString().equals("bazhen"));
		try {
			string.asLong();
			Assert.assertTrue(false);

		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
		try {
			string.asDouble();
			Assert.assertTrue(false);

		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
		try {
			string.asDate();
			Assert.assertTrue(false);

		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		Assert.assertTrue(new String(string.asString().getBytes())
				.equals("bazhen"));
	}

	@Test
	public void test_bool() {
		StringColumn string = new StringColumn("true");
		Assert.assertTrue(string.asString().equals("true"));
		Assert.assertTrue(string.asBoolean().equals(true));

		try {
			string.asDate();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			string.asDouble();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			string.asLong();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
	}

	@Test
	public void test_null() throws UnsupportedEncodingException {
		StringColumn string = new StringColumn();
		Assert.assertTrue(string.asString() == null);
		Assert.assertTrue(string.asLong() == null);
		Assert.assertTrue(string.asDouble() == null);
		Assert.assertTrue(string.asDate() == null);
		Assert.assertTrue(string.asBytes() == null);

	}
}
