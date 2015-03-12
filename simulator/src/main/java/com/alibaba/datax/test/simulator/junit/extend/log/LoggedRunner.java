package com.alibaba.datax.test.simulator.junit.extend.log;

import java.lang.reflect.Method;

import org.apache.commons.lang3.StringUtils;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

public class LoggedRunner extends BlockJUnit4ClassRunner {

	private static String MARK = StringUtils.repeat("====", 10);

	public LoggedRunner(Class<?> klass) throws InitializationError {
		super(klass);
	}

	@Override
	protected Statement methodBlock(FrameworkMethod method) {
		Method classMethod = method.getMethod();
		TestLogger loggerAnnotation = classMethod
				.getAnnotation(TestLogger.class);
		if (loggerAnnotation != null) {
			StringBuilder log = new StringBuilder();
			log.append("\n\n").append(MARK).append(classMethod.getName())
					.append("--" + loggerAnnotation.log()).append(MARK);
			System.out.println(log.toString());
		}
		return super.methodBlock(method);
	}
}