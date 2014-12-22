package com.alibaba.datax.core.scheduler.standalone;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.scheduler.processinner.ProcessInnerScheduler;
import com.alibaba.datax.core.job.scheduler.processinner.StandAloneScheduler;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.statistics.container.communicator.job.LocalJobContainerCommunicator;
import com.alibaba.datax.core.statistics.container.communicator.job.StandAloneJobContainerCommunicator;
import com.alibaba.datax.core.util.ReflectUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.ExecuteMode;
import com.alibaba.datax.dataxservice.face.domain.State;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.Matchers.any;

/**
 * Created by jingxing on 14-9-2.
 */
public class StandAloneSchedulerTest extends CaseInitializer {
	private int randomSize = 20;

	@Test
	public void testSchedule() throws NoSuchFieldException, IllegalAccessException {
		int taskNumber = 10;
		List<Configuration> jobList = new ArrayList<Configuration>();

		List<Configuration> internal = new ArrayList<Configuration>();
		int length = RandomUtils.nextInt(0, randomSize)+1;
		for (int i = 0; i < length; i++) {
			internal.add(Configuration.newDefault());
		}

        LocalTGCommunicationManager.clear();
		for (int i = 0; i < taskNumber; i++) {
			Configuration configuration = Configuration.newDefault();
			configuration
					.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL,
							11);
			configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);
			configuration.set(CoreConstant.DATAX_JOB_CONTENT, internal);
			configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, ExecuteMode.STANDALONE.getValue());
			configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, i);
			jobList.add(configuration);
            LocalTGCommunicationManager.registerTaskGroupCommunication(i,new Communication());
		}

        StandAloneJobContainerCommunicator standAloneJobContainerCommunicator = PowerMockito.
                mock(StandAloneJobContainerCommunicator.class);
        ProcessInnerScheduler scheduler = new StandAloneScheduler(standAloneJobContainerCommunicator);

//        ExecutorService taskGroupContainerExecutorService = PowerMockito.mock(ExecutorService.class);
//        ReflectUtil.setField(scheduler, "taskGroupContainerExecutorService", taskGroupContainerExecutorService);
//        PowerMockito.doNothing().when(taskGroupContainerExecutorService).execute((Runnable) any());
//        PowerMockito.doNothing().when(taskGroupContainerExecutorService).shutdown();
       // PowerMockito.when(scheduler.startAllTaskGroup();)



        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);
        PowerMockito.when(standAloneJobContainerCommunicator.collect()).
                thenReturn(communication);
        PowerMockito.doNothing().when(standAloneJobContainerCommunicator).report(communication);

		scheduler.schedule(jobList);
	}
}
