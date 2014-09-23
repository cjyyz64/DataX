package com.alibaba.datax.core.scheduler.standalone;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scaffold.base.TestInitializer;
import com.alibaba.datax.core.util.CoreConstant;

/**
 * Created by jingxing on 14-9-2.
 */
public class StandAloneSchedulerTest extends TestInitializer {
	private int randomSize = 20;

	@Test
	public void testSchedule() {
		int jobNumber = 10;
		List<Configuration> jobList = new ArrayList<Configuration>();

		List<Configuration> internal = new ArrayList<Configuration>();
		int length = RandomUtils.nextInt(0, randomSize);
		for (int i = 0; i < length; i++) {
			internal.add(Configuration.newDefault());
		}

		for (int i = 0; i < jobNumber; i++) {
			Configuration configuration = Configuration.newDefault();
			configuration
					.set(CoreConstant.DATAX_CORE_CONTAINER_MASTER_REPORTINTERVAL,
							11);
			configuration.set(CoreConstant.DATAX_JOB_CONTENT, internal);
			configuration.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_CLASS,
					StandAloneTestSlaveContainer.class.getName());
			jobList.add(configuration);
		}

		StandAloneScheduler scheduler = new StandAloneScheduler();
		scheduler.schedule(jobList, new StandAloneTestMasterMetric(
				Configuration.newDefault()));
	}
}
