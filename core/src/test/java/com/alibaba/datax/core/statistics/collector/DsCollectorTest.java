package com.alibaba.datax.core.statistics.collector;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.statistics.container.collector.DsCollector;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.core.util.ReflectUtil;
import com.alibaba.datax.dataxservice.face.domain.Result;
import com.alibaba.datax.dataxservice.face.domain.TaskGroup;
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

        ConcurrentHashMap<Integer,Communication> map = new ConcurrentHashMap<Integer, Communication>();
        map.put(taskGroupId_1,new Communication());
        ReflectUtil.setField(new LocalTGCommunicationManager(), "taskGroupCommunicationMap", map);

        Result<List<TaskGroup>> result = new Result<List<TaskGroup>>();
        List<TaskGroup> taskGroups = new ArrayList<TaskGroup>();
        TaskGroup taskGroup = new TaskGroup();
        taskGroup.setJobId(jobId);
        taskGroup.setTaskGroupId(taskGroupId_1);
        taskGroup.setTotalBytes(1024L);
        taskGroups.add(taskGroup);

        Communication communication = new Communication();
        communication.setLongCounter("totalBytes",1024);

        result.setData(taskGroups);
        PowerMockito.mockStatic(DataxServiceUtil.class);
        PowerMockito.when(DataxServiceUtil.getTaskGroupInJob(jobId)).thenReturn(result);
        PowerMockito.when(DataxServiceUtil.convertTaskGroupToCommunication(taskGroup)).thenReturn(communication);

        Communication comm = dsCollector.collectFromTaskGroup();
        Assert.assertTrue(comm.getLongCounter("totalBytes") == 1024);
        System.out.println(comm);
    }
}
