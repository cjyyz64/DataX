package com.alibaba.datax.plugin.rdbms.util;

import java.sql.Connection;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class ColumnRangeSplitTest {
	@Test
	public void test_mysql() {
		Connection connection = DBUtil.getConnection("mysql",
				"jdbc:mysql://10.232.128.67:3306/datax", "root", "root");
		Assert.assertTrue(connection != null);

		List<String> result = null;

		result = ColumnRangeSplit.split(connection, "nova", "sku_id", 1);
		System.out.println(result);
		Assert.assertTrue(result.size() == 2);

		result = ColumnRangeSplit.split(connection, "nova", "sku_id", 10);
		System.out.println(result);
		Assert.assertTrue(result.size() == 11);

		result = ColumnRangeSplit.split(connection, "user", "user_id", 1);
		System.out.println(result);
		Assert.assertTrue(result.size() == 2);

		result = ColumnRangeSplit.split(connection, "user", "user_id", 10);
		System.out.println(result);
		Assert.assertTrue(result.size() == 2);

		result = ColumnRangeSplit.split(connection, "table3", "column0", 10);
		System.out.println(result);
		Assert.assertTrue(result.size() == 1);

		result = ColumnRangeSplit
				.split(connection, "bazhen_profiler", "pk", 10);
		System.out.println(result);
		Assert.assertTrue(result.size() == 11);

		result = ColumnRangeSplit.split(connection, "item_bi_baomdistance",
				"taobao_first_category_name", 1000);
		System.out.println(result);
		Assert.assertTrue(result.size() == 1001);

	}
}
