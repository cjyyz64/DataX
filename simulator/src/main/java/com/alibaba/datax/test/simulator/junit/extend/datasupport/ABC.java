package com.alibaba.datax.test.simulator.junit.extend.datasupport;

import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataGeneratorJUnit4ClassRunner.class)
@DataGeneratorConfig(dbConfig = "1.conf", textFiles = "abc.txt")
public class ABC {

	@Test
	public void testBasic() {
		System.out.println(DataGenerator.getCacheData());
	}
}
