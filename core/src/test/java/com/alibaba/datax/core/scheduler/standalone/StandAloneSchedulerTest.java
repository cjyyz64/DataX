package com.alibaba.datax.core.scheduler.standalone;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.core.statistics.collector.container.standalone.TaskGroupContainerCollector;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.util.CoreConstant;

/**
 * Created by jingxing on 14-9-2.
 */
public class StandAloneSchedulerTest extends CaseInitializer {
	private int randomSize = 20;

	@Test
	public void testSchedule() {
		int jobNumber = 10;
		List<Configuration> jobList = new ArrayList<Configuration>();

		List<Configuration> internal = new ArrayList<Configuration>();
		int length = RandomUtils.nextInt(0, randomSize)+1;
		for (int i = 0; i < length; i++) {
			internal.add(Configuration.newDefault());
		}

		for (int i = 0; i < jobNumber; i++) {
			Configuration configuration = Configuration.newDefault();
			configuration
					.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL,
							11);
			configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);
			configuration.set(CoreConstant.DATAX_JOB_CONTENT, internal);
			configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CLASS,
					StandAloneTestTaskGroupContainer.class.getName());
			configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, i);
			configuration
					.set(CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_CONTAINER_TASKGROUPCLASS,
							TaskGroupContainerCollector.class.getName());
			jobList.add(configuration);

		}

		StandAloneScheduler scheduler = new StandAloneScheduler();
		scheduler.schedule(jobList, new StandAloneTestJobCollector(
				Configuration.newDefault()));
	}
}
