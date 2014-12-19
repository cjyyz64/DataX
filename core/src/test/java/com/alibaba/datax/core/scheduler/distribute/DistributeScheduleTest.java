package com.alibaba.datax.core.scheduler.distribute;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.scheduler.DsScheduler;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.communicator.job.DistributeJobContainerCommunicator;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.ReflectUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.Result;
import com.alibaba.datax.dataxservice.face.domain.State;
import com.alibaba.datax.dataxservice.face.domain.TaskGroup;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Matchers.*;


/**
 * Created by hongjiao.hj on 2014/12/17.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DataxServiceUtil.class)
public class DistributeScheduleTest {


    @Test
    public void testStartAllTaskGroup() {
        PowerMockito.mockStatic(DataxServiceUtil.class);

        Result result = new Result();
        result.setData("启动成功");
        result.setReturnCode(200);
        result.isSuccess();
        PowerMockito.when(DataxServiceUtil.startTaskGroup(anyLong(), any(TaskGroup.class))).
                thenReturn(result);
        DsScheduler scheduler = PowerMockito.mock(DsScheduler.class);

        List<Configuration> taskGroupConfigurations = new ArrayList<Configuration>();
        Configuration configuration = PowerMockito.mock(Configuration.class);
        taskGroupConfigurations.add(configuration);
        taskGroupConfigurations.add(configuration);
        scheduler.startAllTaskGroup(taskGroupConfigurations);
    }

    @Test
    public void testDealFailedStat() throws NoSuchFieldException, IllegalAccessException {
        PowerMockito.mockStatic(DataxServiceUtil.class);

        Result result = new Result();
        result.setData("KILL成功");
        result.setReturnCode(200);
        result.isSuccess();
        PowerMockito.when(DataxServiceUtil.killTaskGroup(anyLong(), anyInt())).
                thenReturn(result);

        DsScheduler scheduler = PowerMockito.mock(DsScheduler.class);
        Configuration configuration = PowerMockito.mock(Configuration.class);

        DistributeJobContainerCommunicator containerCollector = new DistributeJobContainerCommunicator(configuration);

        ConcurrentHashMap<Integer,Communication> map = new ConcurrentHashMap<Integer,Communication>();
        Communication communication = new Communication();
        communication.setState(State.RUNNING);
        map.put(1, communication);

        ReflectUtil.setField(containerCollector,"taskGroupCommunicationMap",map);

        scheduler.dealFailedStat(containerCollector, null);
    }

    @Test
    public void testDealKillingStat() throws NoSuchFieldException, IllegalAccessException {
        PowerMockito.mockStatic(DataxServiceUtil.class);

        Result result = new Result();
        result.setData("KILL成功");
        result.setReturnCode(200);
        result.isSuccess();
        PowerMockito.when(DataxServiceUtil.killTaskGroup(anyLong(), anyInt())).
                thenReturn(result);

        DsScheduler scheduler = PowerMockito.mock(DsScheduler.class);
        Configuration configuration = PowerMockito.mock(Configuration.class);

        DistributeJobContainerCommunicator containerCollector = new DistributeJobContainerCommunicator(configuration);

        ConcurrentHashMap<Integer,Communication> map = new ConcurrentHashMap<Integer,Communication>();
        Communication communication = new Communication();
        communication.setState(State.RUNNING);
        map.put(1, communication);

        ReflectUtil.setField(containerCollector,"taskGroupCommunicationMap",map);

        try {
            scheduler.dealKillingStat(containerCollector, 2);
        } catch (DataXException e) {
            Assert.assertTrue(e.getErrorCode().equals(FrameworkErrorCode.KILL_JOB_TIMEOUT_ERROR));
        }
    }

    @Test
    public void testSchedule() {
        int taskGraoupNumber = 10;

        DistributeJobContainerCommunicator distributeJobContainerCommunicator = PowerMockito.
                mock(DistributeJobContainerCommunicator.class);
        DsScheduler scheduler = new DsScheduler(distributeJobContainerCommunicator);

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

        PowerMockito.mockStatic(DataxServiceUtil.class);
        Result result = new Result();
        result.setData("启动成功");
        result.setReturnCode(200);
        PowerMockito.when(DataxServiceUtil.startTaskGroup(anyLong(), any(TaskGroup.class))).
                thenReturn(result);

        Result<Integer> result2 = new Result<Integer>();
        result2.setData(0);
        result2.setReturnCode(200);
        result2.isSuccess();
        PowerMockito.when(DataxServiceUtil.getJobInfo(anyLong())).
                thenReturn(result2);


        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);
        PowerMockito.when(distributeJobContainerCommunicator.collect()).
                thenReturn(communication);

        scheduler.schedule(configurationList);

    }
}
