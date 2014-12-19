package com.alibaba.datax.core.statistics.container;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.dataxservice.face.domain.State;

import java.util.List;
import java.util.Map;

public interface ContainerCommunicator {
    void registerCommunication(List<Configuration> configurationList);

    Communication collect();

    void report(Communication communication);

    State collectState();

    Communication getCommunication(Integer id);

    List<Communication> getCommunications(List<Integer> ids);

    /**
     * 当 实现是 TGContainerCommunicator 时，返回的 Map: key=taskId, value=Communication
     * 当 实现是 JobContainerCommunicator 时，返回的 Map: key=taskGroupId, value=Communication
     */
    Map<Integer, Communication> getCommunicationMap();
}
