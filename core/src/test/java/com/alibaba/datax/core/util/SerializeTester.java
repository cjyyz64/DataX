package com.alibaba.datax.core.util;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.fastjson.JSON;

public class SerializeTester {
	@Test
	public void test_exception() {
		Exception exception = DataXException.asDataXException(
				FrameworkErrorCode.RUNTIME_ERROR, "ERROR");
		String ex = JSON.toJSONString(exception);
		System.out.println(ex);

		Exception another = JSON.parseObject(ex, Exception.class);
		System.out.println(another);
	}

	// 测试特殊字符序列化反序列化为Conf，是否能够保持数据不失真
	@Test
	public void test_serialize() {
		Configuration configuration = Configuration.from("{\"a\": \"b\nb\"}");
		System.out.println(configuration.get("a"));
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		sb.append((char) 1);
		sb.append((char) 127);
		sb.append("\t");
		configuration.set("a", sb.toString());
		System.out.println("A is : " + configuration.getString("a"));

		Configuration another = Configuration.from(configuration.toJSON());
		System.out.println(another.toJSON());

		Assert.assertTrue(another.get("a").equals(sb.toString()));
	}
}
