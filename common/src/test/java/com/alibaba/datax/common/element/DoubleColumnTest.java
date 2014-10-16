package com.alibaba.datax.common.element;

import java.math.BigDecimal;

import org.junit.Assert;
import org.junit.Test;

public class DoubleColumnTest {
	@Test
	public void test_null() {
		DoubleColumn column = new DoubleColumn();

		System.out.println(column.asString());
		Assert.assertTrue(column.asString() == null);
		System.out.println(column.toString());
		Assert.assertTrue(column.toString().equals(
				"{\"byteSize\":0,\"type\":\"DOUBLE\"}"));
		Assert.assertTrue(column.asDouble() == null);
		Assert.assertTrue(column.asString() == null);

		try {
			Assert.assertTrue(column.asBoolean() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		try {
			Assert.assertTrue(column.asDate() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_double() {
		DoubleColumn column = new DoubleColumn(1.0d);

		System.out.println(column.asString());
		Assert.assertTrue(column.asString().equals("1.0"));
		System.out.println(column.toString());
		Assert.assertTrue(column.toString().equals(
				"{\"byteSize\":8,\"rawData\":\"1.0\",\"type\":\"DOUBLE\"}"));

		System.out.println(column.asDouble());
		Assert.assertTrue(column.asDouble().equals(1.0d));

		try {
			Assert.assertTrue(column.asBoolean() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_float() {
		DoubleColumn column = new DoubleColumn(1.0f);

		System.out.println(column.asString());
		Assert.assertTrue(column.asString().equals("1.0"));
		System.out.println(column.toString());
		Assert.assertTrue(column.toString().equals(
				"{\"byteSize\":4,\"rawData\":\"1.0\",\"type\":\"DOUBLE\"}"));

		System.out.println(column.asDouble());
		Assert.assertTrue(column.asDouble().equals(1.0d));

		try {
			Assert.assertTrue(column.asBoolean() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_string() {
		DoubleColumn column = new DoubleColumn("1.0");

		System.out.println(column.asString());
		Assert.assertTrue(column.asString().equals("1.0"));
		System.out.println(column.toString());
		Assert.assertTrue(column.toString().equals(
				"{\"byteSize\":3,\"rawData\":\"1.0\",\"type\":\"DOUBLE\"}"));

		System.out.println(column.asDouble());
		Assert.assertTrue(column.asDouble().equals(1.0d));

		try {
			Assert.assertTrue(column.asBoolean() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_BigDecimal() {
		DoubleColumn column = new DoubleColumn(new BigDecimal("1E-100"));

		System.out.println(column.asString());
		System.out.println(column.asString().length());
		Assert.assertTrue(column.asString().length() == 102);

		Assert.assertTrue(column.asString().equals(
				new BigDecimal("1E-100").toPlainString()));

		Assert.assertTrue(column
				.toString()
				.equals("{\"byteSize\":102,\"rawData\":\"0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001\",\"type\":\"DOUBLE\"}"));

		System.out.println(column.asDouble());
		Assert.assertTrue(column.asDouble().equals(1.0E-100));

		try {
			Assert.assertTrue(column.asBoolean() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}
}
