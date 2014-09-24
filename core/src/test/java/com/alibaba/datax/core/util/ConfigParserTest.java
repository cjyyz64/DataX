package com.alibaba.datax.core.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;

public class ConfigParserTest extends CaseInitializer {
	@Test
	public void test() throws URISyntaxException {
		String path = ConfigParserTest.class.getClassLoader()
				.getResource(".").getFile();
		Configuration configuration = ConfigParser.parse(path + File.separator
				+ "job" + File.separator + "job.json");
		System.out.println(configuration.toJSON());

		Assert.assertTrue(configuration.getList("job.content").size() == 2);
		Assert.assertTrue(configuration.getString("job.content[0].reader.type")
				.equals("fakereader"));
		Assert.assertTrue(configuration.getString("job.content[1].reader.type")
				.equals("fakereader"));
		Assert.assertTrue(configuration.getString("job.content[0].writer.type")
				.equals("fakewriter"));
		Assert.assertTrue(configuration.getString("job.content[1].writer.type")
				.equals("fakewriter"));

		System.out.println(configuration.getConfiguration("plugin").toJSON());

		configuration = configuration.getConfiguration("plugin");
		Assert.assertTrue(configuration.getString("reader.fakereader.name")
				.equals("fakereader"));
		Assert.assertTrue(configuration.getString("writer.fakewriter.name")
				.equals("fakewriter"));

	}
}
