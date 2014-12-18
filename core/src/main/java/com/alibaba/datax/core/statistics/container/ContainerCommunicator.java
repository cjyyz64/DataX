package com.alibaba.datax.core.statistics.container;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.dataxservice.face.domain.State;

import java.util.List;
import java.util.Map;

public interface ContainerCommunicator {
    void registerCommunication(List<Configuration> configurationList);

    void report(Communication communication);

    Communication collect();

    State collectState();

    Communication getCommunication(int id);

    List<Communication> getCommunications(List<Integer> ids);

    Map<Integer, Communication> getCommunicationsMap();
}
