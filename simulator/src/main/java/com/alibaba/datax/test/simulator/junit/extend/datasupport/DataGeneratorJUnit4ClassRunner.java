package com.alibaba.datax.test.simulator.junit.extend.datasupport;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

public class DataGeneratorJUnit4ClassRunner extends BlockJUnit4ClassRunner {

	public DataGeneratorJUnit4ClassRunner(Class<?> clazz)
			throws InitializationError {
		super(clazz);
	}

	@Override
	public void run(RunNotifier notifier) {
		// 在运行前对DataGenerator进行初始化
		initGenerator();
		super.run(notifier);
	}

	/**
	 * 初始化DataGenerator
	 */
	private void initGenerator() {
		Class<?> clazz = getTestClass().getJavaClass();
		while (clazz != null) {
			DataGeneratorConfig annotation = clazz
					.getAnnotation(DataGeneratorConfig.class);

			if (annotation != null) {
				String dbConfig = annotation.dbConfig();
				String[] textFiles = annotation.textFiles();

				try {
					DataGenerator.initCache(
							getAbsoluteTextFilesPaths(textFiles),
							getAbsolutePath(dbConfig));
				} catch (Exception e) {
					throw new RuntimeException("使用注解初始化DataGenerator失败", e);
				}
				break;
			}

			clazz = clazz.getSuperclass();
		}
	}

	/**
	 * 取得text文件绝对路径
	 */
	private String[] getAbsoluteTextFilesPaths(String[] excelPaths) {
		String[] realPaths = new String[excelPaths.length];
		for (int i = 0; i < excelPaths.length; i++) {
			realPaths[i] = getAbsolutePath(excelPaths[i]);
		}
		return realPaths;
	}

	/**
	 * 根据文件名取得文件绝对路径
	 */
	private String getAbsolutePath(String fileName) {
		return DataGeneratorJUnit4ClassRunner.class.getClassLoader()
				.getResource(fileName).getFile();
	}
}