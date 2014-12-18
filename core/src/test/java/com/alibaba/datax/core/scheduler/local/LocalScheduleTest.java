package com.alibaba.datax.core.scheduler.local;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.TaskGroupContainer;

import com.alibaba.datax.core.statistics.collector.container.distribute.DistributeTaskGroupContainerCollector;
import com.alibaba.datax.core.statistics.collector.container.local.LocalJobContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.util.CoreConstant;

import com.alibaba.datax.dataxservice.face.domain.State;

import org.apache.commons.lang3.RandomUtils;

import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by hongjiao.hj on 2014/12/17.
 */
public class LocalScheduleTest {

    @Test
    public void testSchedule() throws Exception {
        int taskGraoupNumber = 10;

        LocalScheduler scheduler = new LocalScheduler();

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
            configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CLASS,
                    TaskGroupContainer.class.getName());
            configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, i);
            configuration
                    .set(CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_CONTAINER_TASKGROUPCLASS,
                            DistributeTaskGroupContainerCollector.class.getName());
            configurationList.add(configuration);
        }

        LocalJobContainerCollector jobContainerCollector =
                PowerMockito.mock(LocalJobContainerCollector.class);


        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);
        PowerMockito.when(jobContainerCollector.collect()).
                thenReturn(communication);
        PowerMockito.doNothing().when(jobContainerCollector).report(communication);
        scheduler.schedule(configurationList,jobContainerCollector);
    }
}
