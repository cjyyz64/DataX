package com.alibaba.datax.common.element;

import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ColumnCastTest {
	private Configuration produce() throws IOException {
		String path = ColumnCastTest.class.getClassLoader().getResource(".")
				.getFile();
		String content = FileUtils.readFileToString(new File(StringUtils.join(
				new String[] { path, "all.json" }, File.separator)));
		return Configuration.from(content);
	}

	@Test
	public void test_string() throws IOException {
		Assert.assertTrue(StringCast.asBoolean(new StringColumn("true"))
				.booleanValue());
		Assert.assertTrue(StringCast.asBoolean(new StringColumn("True"))
				.booleanValue());
		Assert.assertFalse(StringCast.asBoolean(new StringColumn("false"))
				.booleanValue());
		Assert.assertFalse(StringCast.asBoolean(new StringColumn("False"))
				.booleanValue());

		Configuration configuration = this.produce();
		StringCast.init(configuration);

		Assert.assertTrue(StringCast.asBoolean(new StringColumn("true"))
				.booleanValue());
		Assert.assertTrue(StringCast.asBoolean(new StringColumn("True"))
				.booleanValue());
		Assert.assertFalse(StringCast.asBoolean(new StringColumn("false"))
				.booleanValue());
		Assert.assertFalse(StringCast.asBoolean(new StringColumn("False"))
				.booleanValue());

		configuration.set("data.column.string.bool.yes", true);
		configuration.set("data.column.string.bool.^yes", false);
		StringCast.init(configuration);

		Assert.assertTrue(StringCast.asBoolean(new StringColumn("yes"))
				.booleanValue());
		Assert.assertTrue(StringCast.asBoolean(new StringColumn("Yes"))
				.booleanValue());
		Assert.assertFalse(StringCast.asBoolean(new StringColumn("No"))
				.booleanValue());
		Assert.assertFalse(StringCast.asBoolean(new StringColumn("NO"))
				.booleanValue());

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

	@Test
	public void test_bool() throws IOException {
		Assert.assertTrue(BoolCast.asLong(new BoolColumn(true)) == 1L);
		Assert.assertTrue(BoolCast.asLong(new BoolColumn(false)) == 0L);
		Assert.assertTrue(BoolCast.asString(new BoolColumn(true))
				.equals("true"));
		Assert.assertTrue(BoolCast.asString(new BoolColumn(false)).equals(
				"false"));

		Configuration configuration = this.produce();
		configuration.set("data.column.bool.number.true", Integer.MAX_VALUE);
		configuration.set("data.column.bool.number.false", Integer.MIN_VALUE);
		configuration.set("data.column.bool.string.true", "yes");
		configuration.set("data.column.bool.string.false", "no");
		BoolCast.init(configuration);

		Assert.assertTrue(BoolCast.asString(new BoolColumn(true)).equals("yes"));
		Assert.assertTrue(BoolCast.asString(new BoolColumn(false)).equals("no"));
		Assert.assertTrue(BoolCast.asInteger(new BoolColumn(true)).equals(
				Integer.MAX_VALUE));
		Assert.assertTrue(BoolCast.asInteger(new BoolColumn(false)).equals(
				Integer.MIN_VALUE));
	}
}
