package com.alibaba.datax.common.element;

import com.alibaba.datax.common.util.Configuration;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

public class ColumnCastTest {
	private Configuration produce() throws IOException {
		String path = ColumnCastTest.class.getClassLoader().getResource(".")
				.getFile();
		String content = FileUtils.readFileToString(new File(StringUtils.join(
				new String[] { path, "all.json" }, File.separator)));
		return Configuration.from(content);
	}

	@Test
	public void test_string() throws IOException, ParseException {
		Configuration configuration = this.produce();
		StringCast.init(configuration);

		Assert.assertTrue(StringCast.asDate(
				new StringColumn("2014-09-18 16:00:00")).getTime() == 1411027200000L);
		configuration.set("data.column.string.date.timeFormat",
				"yyyy/MM/dd HH:mm:ss");
		StringCast.init(configuration);
		Assert.assertTrue(StringCast.asDate(
				new StringColumn("2014/09/18 16:00:00")).getTime() == 1411027200000L);
	}

	@Test
	public void test_date() throws IOException {
		Assert.assertTrue(DateCast.asString(
				new DateColumn(System.currentTimeMillis())).startsWith("201"));

		Configuration configuration = this.produce();
		configuration.set("data.column.date.string.timeFormat",
				"MM/dd/yyyy HH:mm:ss");
		DateCast.init(configuration);
		System.out.println(DateCast.asString(new DateColumn(System
				.currentTimeMillis())));
		Assert.assertTrue(!DateCast.asString(
				new DateColumn(System.currentTimeMillis())).startsWith("2014"));
	}
}
