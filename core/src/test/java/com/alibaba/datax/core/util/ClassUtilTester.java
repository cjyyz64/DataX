package com.alibaba.datax.core.util;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.AbstractContainer;

public final class ClassUtilTester {
	@Test
	public void test() {

		Assert.assertTrue(ClassUtil.instantiate(
                Dummy.class.getCanonicalName(), Dummy.class) != null);

		Dummy dummy = ClassUtil.instantiate(Dummy.class.getCanonicalName(),
                Dummy.class);
		Assert.assertTrue(dummy instanceof Dummy);

		Assert.assertTrue(ClassUtil.instantiate(
                DummyContainer.class.getCanonicalName(), DummyContainer.class,
                Configuration.from("{}")) instanceof DummyContainer);

		Assert.assertTrue(ClassUtil.instantiate(
                DummyContainer.class.getCanonicalName(), DummyContainer.class,
                Configuration.from("{}")) instanceof DummyContainer);
	}
}

class DummyContainer extends AbstractContainer {
	public DummyContainer(Configuration configuration) {
		super(configuration);
	}

	@Override
	public void start() {
		System.out.println(getConfiguration());
	}
}

class Dummy {
	public Dummy() {
	}
}
