package com.alibaba.datax.test.simulator.junit.extend.datasupport;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface DataGeneratorConfig {
	/**
	 * jdbc配置文件
	 */
	String dbConfig() default "db.config";

	/**
	 * text文件列表
	 */
	String[] textFiles();
}