package com.alibaba.datax.core.container.runner;

import com.alibaba.datax.common.plugin.AbstractSlavePlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jingxing on 14-9-1.
 */
public class RunnerManager {
    private static Map<Integer, List<AbstractRunner>> runners =
            new HashMap<Integer, List<AbstractRunner>>();

    private static void initRunner(final int slaveId) {
        if (runners.get(slaveId) == null) {
            runners.put(slaveId,
                    new ArrayList<AbstractRunner>());
        }
    }

    private static synchronized void registerRunner(final int slaveId,
                                                    AbstractRunner runner) {
        RunnerManager.initRunner(slaveId);
        runners.get(slaveId).add(runner);
    }

    public static synchronized List<AbstractRunner> getRunners(int slaveId) {
        RunnerManager.initRunner(slaveId);
        return runners.get(slaveId);
    }

    public static synchronized List<AbstractRunner> getRunners() {
        List<AbstractRunner> results = new ArrayList<AbstractRunner>();

        for (final int slaveId : runners.keySet()) {
            results.addAll(runners.get(slaveId));
        }

        return results;
    }

    public static synchronized void clear() {
        runners.clear();
    }

    public static ReaderRunner newReaderRunner(
            AbstractSlavePlugin abstractSlavePlugin, int slaveId) {
        ReaderRunner readerRunner = new ReaderRunner(abstractSlavePlugin);
        registerRunner(slaveId, readerRunner);
        return readerRunner;
    }

    public static WriterRunner newWriterRunner(AbstractSlavePlugin abstractSlavePlugin,
                                               int slaveId) {
        WriterRunner writerRunner = new WriterRunner(abstractSlavePlugin);
        registerRunner(slaveId, writerRunner);
        return writerRunner;
    }
}
