package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.util.DataxServiceUtil;
import com.alibaba.datax.dataxservice.face.domain.TaskGroupStatusDto;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DsCollector extends AbstractCollector {

    private static final Logger LOG = LoggerFactory
            .getLogger(DsCollector.class);

    public DsCollector(Long jobId) {
        super.setJobId(jobId);
    }

    //是否已经打印从DS获得的失败和kill掉的tg
    private static boolean isPrintFailOrKillTG = false;

    @Override
    public Communication collectFromTaskGroup() {
        /*List<TaskGroup> taskGroupInJob = DataxServiceUtil.getTaskGroupInJob(super.getJobId()).getData();
        for (TaskGroup taskGroup : taskGroupInJob) {
            LocalTGCommunicationManager.updateTaskGroupCommunication(taskGroup.getTaskGroupId(),
                    DataxServiceUtil.convertTaskGroupToCommunication(taskGroup));
        }
        return LocalTGCommunicationManager.getJobCommunication();*/

        //只需要获取tg状态信息，不需要整个tg信息，预防conifg内容过大，导致ds数据库压力。
        List<TaskGroupStatusDto> taskGroupStatusList = DataxServiceUtil.getTaskGroupStatusInJob(super.getJobId()).getData();

        boolean hasFailedOrKilledTG = false;
        for (TaskGroupStatusDto taskGroupStatus : taskGroupStatusList) {
            if(!isPrintFailOrKillTG) {
                if (taskGroupStatus.getState().equals(State.FAILED)) {
                    hasFailedOrKilledTG = true;
                    LOG.error("taskGroup[{}]运行失败.", taskGroupStatus.getTaskGroupId());
                }
                if (taskGroupStatus.getState().equals(State.KILLED)) {
                    hasFailedOrKilledTG = true;
                    LOG.error("taskGroup[{}]被Kill.", taskGroupStatus.getTaskGroupId());
                }
            }
            LocalTGCommunicationManager.updateTaskGroupCommunication(taskGroupStatus.getTaskGroupId(),
                    DataxServiceUtil.convertTaskGroupToCommunication(taskGroupStatus));
        }
        if(hasFailedOrKilledTG) {
            isPrintFailOrKillTG = true;
        }

        return LocalTGCommunicationManager.getJobCommunication();
    }

}
