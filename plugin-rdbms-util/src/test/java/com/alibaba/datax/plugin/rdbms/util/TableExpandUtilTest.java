package com.alibaba.datax.plugin.rdbms.util;

import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class TableExpandUtilTest {

	@Test
	public void test() {
		String[] tableArray = new String[] { "table", "table_[0-1]",
				"table[1-5]", "a,b,c,d_[1-2]", "a,b,schema.cd_[1-5]",
				"schema.a[1-1]_more" };
		List<String> tables = Arrays.asList(tableArray);
		List<String> result = TableExpandUtil.expandTableConf(null, tables);

		String[] target = new String[] { "table", "table_0", "table_1",
				"table1", "table2", "table3", "table4", "table5", "a", "b",
				"c", "d_1", "d_2", "a", "b", "schema.cd_1", "schema.cd_2",
				"schema.cd_3", "schema.cd_4", "schema.cd_5", "schema.a1_more" };

		for (int i = 0; i < result.size(); i++) {
			System.out.print(result.get(i));
			if (i != result.size() - 1) {
				System.out.print(",");
			}
		}
		Assert.assertArrayEquals(result.toArray(), target);
	}
}
