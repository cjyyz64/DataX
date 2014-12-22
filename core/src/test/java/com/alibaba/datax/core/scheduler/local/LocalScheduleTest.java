package com.alibaba.datax.core.scheduler.local;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.scheduler.processinner.LocalScheduler;
import com.alibaba.datax.core.job.scheduler.processinner.ProcessInnerScheduler;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.communicator.job.LocalJobContainerCommunicator;
import com.alibaba.datax.core.util.ReflectUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.State;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.Matchers.*;


/**
 * Created by hongjiao.hj on 2014/12/17.
 */
public class LocalScheduleTest {

    @Test
    public void testSchedule() throws Exception {
        int taskGraoupNumber = 10;

        LocalJobContainerCommunicator localJobContainerCommunicator = PowerMockito.
                mock(LocalJobContainerCommunicator.class);

        ProcessInnerScheduler scheduler = PowerMockito.spy(new LocalScheduler(localJobContainerCommunicator));

        List<Configuration> configurationList = new ArrayList<Configuration>();

        List<Configuration> configurations = new ArrayList<Configuration>();


        int length = RandomUtils.nextInt(0, 20)+1;
        for (int i = 0; i < length; i++) {
            configurations.add(Configuration.newDefault());
        }

        for (int i = 0; i < taskGraoupNumber; i++) {
            Configuration configuration = Configuration.newDefault();
            configuration
                    .set(CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL,
                            11);
            configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);
            configuration.set(CoreConstant.DATAX_JOB_CONTENT, configurations);

            configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, i);

            configurationList.add(configuration);
        }

        PowerMockito.doNothing().when(scheduler).startAllTaskGroup(anyListOf(Configuration.class));

        Communication communication = new Communication();
        communication.setLongCounter("totalBytes",1024);
        communication.setState(State.SUCCEEDED);
        PowerMockito.when(localJobContainerCommunicator.collect()).
                thenReturn(communication);
        PowerMockito.doNothing().when(localJobContainerCommunicator).report(communication);
        scheduler.schedule(configurationList);
    }
}
