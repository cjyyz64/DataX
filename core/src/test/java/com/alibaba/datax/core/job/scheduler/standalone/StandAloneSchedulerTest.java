package com.alibaba.datax.core.job.scheduler.standalone;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.common.ExecuteMode;
import com.alibaba.datax.core.job.scheduler.ProcessInnerScheduler;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.statistics.container.communicator.job.StandAloneJobContainerCommunicator;
import com.alibaba.datax.core.common.CoreConstant;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
			configuration.set("runMode", ExecuteMode.STANDALONE.getValue());
			configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, i);
			jobList.add(configuration);

		}

		//TODO mock
		ProcessInnerScheduler scheduler = new ProcessInnerScheduler();
		scheduler.schedule(jobList, new StandAloneJobContainerCommunicator(Configuration.newDefault()));
	}
}
