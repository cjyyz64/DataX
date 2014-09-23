package com.alibaba.datax.plugin.reader.mysqlreader;

import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.datax.plugin.rdbms.util.SqlFormatUtil;

@Ignore
public class SqlFormatUtilTest {

	@Test
	public void testBasic() {
		String sql = "select `a`,`\"b\"`,c,left(k\"ey,1) from `hello` where 1=1 and id<100 order by hello desc;";
//		String formattedSql = SqlFormatUtil.format(sql);
//		System.out.println(formattedSql);
	}

	// Both Fail
	@Test
	public void testInsert_00() {
		String sql = "insert into `world`(`a`,`b`,c,left(key,1)) values(\"a\"a\");";
		System.out.println(sql);
		try {
			String formattedSql = SqlFormatUtil.format(sql);
			System.out.println(formattedSql);
		} catch (Exception e) {
			e.printStackTrace();
		}

//		try {
//			sql = new BlancoSqlFormatter(new BlancoSqlRule()).format(sql
//					.toString());
//			System.out.println(sql);
//		} catch (BlancoSqlFormatterException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
	}

	@Test
	public void testInsert_01() {
		String sql = "insert into `world`(`a`,`b`,c,left(key,1)) values(\"aa\");";
		System.out.println(sql);
		String formattedSql = SqlFormatUtil.format(sql);
		System.out.println(formattedSql);
	}
}
