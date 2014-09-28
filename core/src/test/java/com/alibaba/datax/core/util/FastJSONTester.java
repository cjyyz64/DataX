package com.alibaba.datax.core.util;

import org.junit.Test;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.fastjson.JSON;

public class FastJSONTester {
	@Test
	public void test() {
		Exception exception = new DataXException(
				FrameworkErrorCode.INNER_ERROR, "ERROR");
		String ex = JSON.toJSONString(exception);
		System.out.println(ex);

		Exception another = JSON.parseObject(ex, Exception.class);
		System.out.println(another);
	}
}
