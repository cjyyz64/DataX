package com.alibaba.datax.core.container;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.core.statistics.collector.container.ContainerCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalSlaveContainerCommunication;
import com.alibaba.datax.core.util.State;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.util.LoadUtil;
import com.alibaba.datax.core.faker.FakeExceptionReader;
import com.alibaba.datax.core.faker.FakeExceptionWriter;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.CoreConstant;

/**
 * Created by jingxing on 14-9-4.
 */
public class SlaveContainerTest extends CaseInitializer {
    private Configuration configuration;
    private int slaveNumber;

    @Before
    public void setUp() {
        String path = SlaveContainerTest.class.getClassLoader()
                .getResource(".").getFile();

        this.configuration = ConfigParser.parse(path + File.separator
                + "all.json");
        LoadUtil.bind(this.configuration);

        int channelNumber = 5;
        slaveNumber = channelNumber + 3;
        this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_MASTER_ID, 0);
        this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_ID, 1);
        this.configuration.set(
                CoreConstant.DATAX_CORE_CONTAINER_SLAVE_SLEEPINTERVAL, 200);
        this.configuration.set(
                CoreConstant.DATAX_CORE_CONTAINER_SLAVE_REPORTINTERVAL, 1000);
        this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_SLAVE_CHANNEL,
                channelNumber);
        Configuration jobContent = this.configuration.getListConfiguration(
                CoreConstant.DATAX_JOB_CONTENT).get(0);
        List<Configuration> jobContents = new ArrayList<Configuration>();
        for (int i = 0; i < this.slaveNumber; i++) {
            Configuration newJobContent = jobContent.clone();
            newJobContent.set(CoreConstant.JOB_TASKID, i);
            jobContents.add(newJobContent);
        }
        this.configuration.set(CoreConstant.DATAX_JOB_CONTENT, jobContents);

        LocalSlaveContainerCommunication.clear();
        LocalSlaveContainerCommunication.registerSlaveContainerCommunication(
                1, new Communication());
    }

    @Test
    public void testStart() throws InterruptedException {
        SlaveContainer slaveContainer = new SlaveContainer(this.configuration);
        slaveContainer.start();

        ContainerCollector collector = slaveContainer.getContainerCollector();
        while (true) {
            State totalSlaveState = collector.collectState();
            if(totalSlaveState.isRunning()) {
                Thread.sleep(1000);
            } else {
                break;
            }
        }

        Communication totalSlaveCommunication = collector.collect();
        List<String> messages = totalSlaveCommunication.getMessage("bazhen-reader");
        Assert.assertTrue(!messages.isEmpty());

        messages = totalSlaveCommunication.getMessage("bazhen-writer");
        Assert.assertTrue(!messages.isEmpty());

        messages = totalSlaveCommunication.getMessage("bazhen");
        Assert.assertNull(messages);

        State state = totalSlaveCommunication.getState();

        Assert.assertTrue("task finished", state.equals(State.SUCCESS));
    }

    @Test(expected = RuntimeException.class)
    public void testReaderException() {
        this.configuration.set("plugin.reader.fakereader.class",
                FakeExceptionReader.class.getCanonicalName());
        SlaveContainer slaveContainer = new SlaveContainer(this.configuration);
        slaveContainer.start();
    }

    @Test(expected = RuntimeException.class)
    public void testWriterException() {
        this.configuration.set("plugin.writer.fakewriter.class",
                FakeExceptionWriter.class.getName());
        SlaveContainer slaveContainer = new SlaveContainer(this.configuration);
        slaveContainer.start();
    }
}
