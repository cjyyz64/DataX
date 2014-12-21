package com.alibaba.datax.core.statistics.reporter;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.report.DsReporter;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.dataxservice.face.domain.JobStatus;
import com.alibaba.datax.dataxservice.face.domain.Result;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupStatus;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.*;

/**
 * Created by hongjiao.hj on 2014/12/21.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DataxServiceUtil.class)
public class DsReporterTest {

    @Test
    public void testReportJobCommunication() {
        Long jobId= 0L;
        DsReporter dsReporter = new DsReporter(jobId);

        Communication communication = new Communication();

        final Result result = new Result();

        PowerMockito.mockStatic(DataxServiceUtil.class);
        PowerMockito.when(DataxServiceUtil.updateJobInfo(anyLong(),any(JobStatus.class)))
                .thenAnswer(new Answer<Object>() {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                        result.setReturnCode(200);
                        return result;
                    }
                });
        dsReporter.reportJobCommunication(jobId, communication);
        Assert.assertTrue(result.getReturnCode() == 200);
    }

    @Test
    public void testReportTGCommunication() {
        Long jobId= 0L;
        Integer taskGroupId = 1;
        DsReporter dsReporter = new DsReporter(jobId);

        Communication communication = new Communication();

        final Result result = new Result();

        PowerMockito.mockStatic(DataxServiceUtil.class);
        PowerMockito.when(DataxServiceUtil.updateTaskGroupInfo
                (anyLong(), anyInt(),any(TaskGroupStatus.class)))
                .thenAnswer(new Answer<Object>() {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                        result.setReturnCode(200);
                        return result;
                    }
                });
        dsReporter.reportTGCommunication(taskGroupId, communication);
        Assert.assertTrue(result.getReturnCode()==200);

    }
}
