package com.alibaba.datax.core.manager;

import com.alibaba.datax.service.face.domain.Result;
import com.alibaba.datax.service.face.domain.TaskGroup;
import com.alibaba.datax.service.face.domain.TaskGroupStatus;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.util.List;

/**
 * Created by hongjiao.hj on 2014/12/9.
 */
public interface DataServiceManager {

    /**
     *  job查询自身状态
     * @param jobId
     * @return
     */
    Result<?> getJobInfo(Long jobId);

    /**
     *  job运行时信息更新
     *
     * @param jobId           jobId
     * @param jobObject       jobObject
     * @return                Result<Boolean>
     */
    Result<Boolean> updateJobInfo(Long jobId, String jobObject);

    /**
     * job查询下属taskGroup运行信息
     *
     * @param jobId           jobId
     * @return                Result<TaskGroup>
     */
    Result<List<TaskGroup>> getTaskGroupInJob(Long jobId);

    /**
     * job启动taskGroup
     *
     * @param jobId           jobId
     * @param taskGroup       taskGroup
     * @return                Result<Boolean>
     */
    Result<Boolean> startTaskGroup(Long jobId, TaskGroup taskGroup);

    /**
     * job kill taskGroup
     *
     * @param jobId            jobId
     * @param taskGroupId      taskGroupId
     * @return                 Result<Boolean>
     */
    Result<Boolean> killTaskGroup(Long jobId, Long taskGroupId);


    /**
     * taskGroup运行时信息更新
     *
     * @param jobId             jobId
     * @param taskGroupId       taskGroupId
     * @param taskGroupStatus   要更新的统计信息
     * @return                  Result<Boolean>
     */
    Result<Boolean> updateTaskGroupInfo(Long jobId, Long taskGroupId, TaskGroupStatus taskGroupStatus);

}
