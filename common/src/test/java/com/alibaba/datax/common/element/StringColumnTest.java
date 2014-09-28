package com.alibaba.datax.common.element;

import java.io.UnsupportedEncodingException;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.base.BaseTest;
import com.alibaba.datax.common.exception.DataXException;

public class StringColumnTest extends BaseTest {

	@Test
	public void test_double() {
		StringColumn string = new StringColumn("3.14");
		Assert.assertTrue(string.asString().equals("3.14"));
		Assert.assertTrue(string.asDouble().equals(3.14d));
		Assert.assertTrue(string.asBoolean().equals(false));
		Assert.assertTrue(string.asLong().equals(3L));

		try {
			string.asDate();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
	}

	@Test
	public void test_int() {
		StringColumn string = new StringColumn("3");
		Assert.assertTrue(string.asString().equals("3"));
		Assert.assertTrue(string.asDouble().equals(3.0d));
		Assert.assertTrue(string.asBoolean().equals(false));
		Assert.assertTrue(string.asLong().equals(3L));
		try {
			string.asDate();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
	}

	@Test
	public void test_string() {
		StringColumn string = new StringColumn("bazhen");
		Assert.assertTrue(string.asString().equals("bazhen"));
		try {
			string.asLong();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
		try {
			string.asDouble();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
		try {
			string.asDate();
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
