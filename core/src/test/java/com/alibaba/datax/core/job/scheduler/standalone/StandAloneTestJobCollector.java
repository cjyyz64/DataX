package com.alibaba.datax.core.job.scheduler.standalone;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.collector.container.AbstractContainerCollector;
import com.alibaba.datax.core.util.communication.Communication;
import com.alibaba.datax.dataxservice.face.domain.State;

import java.util.List;
import java.util.Map;

/**
 * Created by jingxing on 14-9-4.
 */
public class StandAloneTestJobCollector extends AbstractContainerCollector {
    public StandAloneTestJobCollector(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        System.out.println("register ok");
    }

    @Override
    public void report(Communication communication) {
        System.out.println("job report 2");
    }

    @Override
    public Communication collect() {
        return new Communication() {{
            this.setState(State.SUCCEEDED);
        }};
    }

    @Override
    public State collectState() {
        return State.SUCCEEDED;
    }

    @Override
    public Communication getCommunication(int id) {
        return null;
    }

    @Override
    public List<Communication> getCommunications(List<Integer> ids) {
        return null;
    }

    @Override
    public Map<Integer, Communication> getCommunicationsMap() {
        return null;
    }
}
