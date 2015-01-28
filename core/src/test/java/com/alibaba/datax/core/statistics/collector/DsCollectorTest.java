package com.alibaba.datax.core.statistics.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.statistics.container.collector.DsCollector;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.util.ReflectUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.Result;
import com.alibaba.datax.dataxservice.face.domain.State;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupStatusDto;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by hongjiao.hj on 2014/12/21.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DataxServiceUtil.class)
public class DsCollectorTest {
    @Test
    public void tesCollectFromTaskGroup() throws NoSuchFieldException, IllegalAccessException {
        Long jobId = 0L;
        Integer taskGroupId_1 = 1;
        DsCollector dsCollector = new DsCollector(jobId);

        ConcurrentHashMap<Integer, Communication> map = new ConcurrentHashMap<Integer, Communication>();
        map.put(taskGroupId_1, new Communication());
        ReflectUtil.setField(new LocalTGCommunicationManager(), "taskGroupCommunicationMap", map);

        Result<List<TaskGroupStatusDto>> result = new Result<List<TaskGroupStatusDto>>();
        List<TaskGroupStatusDto> taskGroupStatusList = new ArrayList<TaskGroupStatusDto>();
        TaskGroupStatusDto taskGroupStatus = new TaskGroupStatusDto();
        taskGroupStatus.setJobId(jobId);
        taskGroupStatus.setTaskGroupId(taskGroupId_1);
        taskGroupStatus.setTotalBytes(1024L);
        taskGroupStatusList.add(taskGroupStatus);

        Communication communication = new Communication();
        communication.setLongCounter("totalBytes", 1024);

        result.setData(taskGroupStatusList);
        PowerMockito.mockStatic(DataxServiceUtil.class);
        PowerMockito.when(DataxServiceUtil.getTaskGroupStatusInJob(jobId)).thenReturn(result);
        PowerMockito.when(DataxServiceUtil.convertTaskGroupToCommunication(taskGroupStatus)).thenReturn(communication);

        Communication comm = dsCollector.collectFromTaskGroup();
        Assert.assertTrue(comm.getLongCounter("totalBytes") == 1024);
        System.out.println(comm);
    }

    @Test
    public void testRegisterTGCommunication() {
        Long jobId = 0L;
        DsCollector dsCollector = new DsCollector(jobId);
        List<Configuration> taskGroupConfigurationList = new ArrayList<Configuration>();

        int tgCount = 5;
        for (int i = 1; i <= tgCount; i++) {
            Configuration configuration = Configuration.from("{}");
            configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, i);
            taskGroupConfigurationList.add(configuration);
        }

        dsCollector.registerTGCommunication(taskGroupConfigurationList);

        Assert.assertTrue(LocalTGCommunicationManager.getTaskGroupCommunicationMap().size() == tgCount);
    }

    @Test
    public void testRegisterTaskCommunication() {
        Long jobId = 0L;
        DsCollector dsCollector = new DsCollector(jobId);
        List<Configuration> taskConfigurationList = new ArrayList<Configuration>();

        int taskCount = 10;
        for (int i = 1; i <= taskCount; i++) {
            Configuration configuration = Configuration.from("{}");
            configuration.set(CoreConstant.TASK_ID, i);
            taskConfigurationList.add(configuration);
        }

        dsCollector.registerTaskCommunication(taskConfigurationList);

        Assert.assertTrue(dsCollector.getTaskCommunicationMap().size() == taskCount);
    }

    @Test
    public void testCollectFromTask() {
        Long jobId = 0L;
        DsCollector dsCollector = new DsCollector(jobId);
        List<Configuration> taskConfigurationList = new ArrayList<Configuration>();

        int taskCount = 10;
        for (int i = 1; i <= taskCount; i++) {
            Configuration configuration = Configuration.from("{}");
            configuration.set(CoreConstant.TASK_ID, i);
            taskConfigurationList.add(configuration);
        }

        // 注册 task
        dsCollector.registerTaskCommunication(taskConfigurationList);

        Communication communication = dsCollector.getTaskCommunication(1);
        communication.setState(State.FAILED);
        communication.setLongCounter("readBytes", 10);
        for (int i = 2; i <= taskCount; i++) {
            communication = dsCollector.getTaskCommunication(i);
            communication.setState(State.RUNNING);
            communication.setLongCounter("readBytes", 100);
        }

        Communication resultCommunication = dsCollector.collectFromTask();
        Assert.assertTrue(resultCommunication.getState() == State.FAILED);
        Assert.assertTrue(resultCommunication.getLongCounter("readBytes") == 9 * 100 + 10);
    }
}
