package com.alibaba.datax.common.statistics;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liqiang on 15/8/26.
 */
public class PerfRecordTest {
    private static Logger LOG = LoggerFactory.getLogger(PerfRecordTest.class);
    private final int TGID = 1;

    @Test
    public void testNormal() throws Exception {

        PerfTrace.getInstance(true, 1001, 1, 0, true);

        PerfRecord initPerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord.start();
        Thread.sleep(1050);
        initPerfRecord.end();

        LOG.debug("task writer starts to do prepare ...");
        PerfRecord preparePerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.WRITE_TASK_PREPARE);
        preparePerfRecord.start();
        Thread.sleep(1020);
        preparePerfRecord.end();
        LOG.debug("task writer starts to write ...");

        PerfRecord dataPerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.READ_TASK_DATA);
        dataPerfRecord.start();

        Thread.sleep(1200);
        dataPerfRecord.addCount(1001);
        dataPerfRecord.addSize(1001);
        dataPerfRecord.end();

        PerfRecord destoryPerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.READ_TASK_DESTROY);
        destoryPerfRecord.start();

        Thread.sleep(250);
        destoryPerfRecord.end();

        PerfRecord initPerfRecord2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord2.start();
        Thread.sleep(50);
        initPerfRecord2.end();

        LOG.debug("task writer starts to do prepare ...");
        PerfRecord preparePerfRecord2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.WRITE_TASK_PREPARE);
        preparePerfRecord2.start();
        Thread.sleep(20);
        preparePerfRecord2.end();
        LOG.debug("task writer starts to write ...");

        PerfRecord dataPerfRecor2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.READ_TASK_DATA);
        dataPerfRecor2.start();

        Thread.sleep(2200);
        dataPerfRecor2.addCount(2001);
        dataPerfRecor2.addSize(2001);
        dataPerfRecor2.end();

        PerfRecord destoryPerfRecord2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.READ_TASK_DESTROY);
        destoryPerfRecord2.start();

        Thread.sleep(1250);
        destoryPerfRecord2.end();

        PerfTrace.getInstance().addTaskDetails(1,"task 1 some thing abcdf");
        PerfTrace.getInstance().addTaskDetails(2,"task 2 some thing abcdf");
        System.out.println(PerfTrace.getInstance().summarize());
    }
    @Test
    public void testDisable() throws Exception {

        PerfTrace.getInstance(true, 1001, 1, 0,false);

        PerfRecord initPerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord.start();
        Thread.sleep(1050);
        initPerfRecord.end();

        LOG.debug("task writer starts to do prepare ...");
        PerfRecord preparePerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.WRITE_TASK_PREPARE);
        preparePerfRecord.start();
        Thread.sleep(1020);
        preparePerfRecord.end();
        LOG.debug("task writer starts to write ...");

        PerfRecord dataPerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.READ_TASK_DATA);
        dataPerfRecord.start();

        Thread.sleep(1200);
        dataPerfRecord.addCount(1001);
        dataPerfRecord.addSize(1001);
        dataPerfRecord.end();

        PerfRecord initPerfRecord2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord2.start();
        Thread.sleep(50);
        initPerfRecord2.end();

        LOG.debug("task writer starts to do prepare ...");
        PerfRecord preparePerfRecord2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.WRITE_TASK_PREPARE);
        preparePerfRecord2.start();
        Thread.sleep(20);
        preparePerfRecord2.end();
        LOG.debug("task writer starts to write ...");

        PerfRecord dataPerfRecor2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.READ_TASK_DATA);
        dataPerfRecor2.start();

        Thread.sleep(2200);
        dataPerfRecor2.addCount(2001);
        dataPerfRecor2.addSize(2001);
        dataPerfRecor2.end();

        PerfTrace.getInstance().addTaskDetails(1, "task 1 some thing abcdf");
        PerfTrace.getInstance().addTaskDetails(2, "task 2 some thing abcdf");
        System.out.println(PerfTrace.getInstance().summarize());
    }

    @Test
    public void testNormal2() throws Exception {
        int priority = 0;
        try {
            priority = Integer.parseInt(System.getenv("SKYNET_PRIORITY"));
        }catch (NumberFormatException e){
            LOG.warn("prioriy set to 0, because NumberFormatException, the value is: "+System.getProperty("PROIORY"));
        }

        System.out.println("priority===="+priority);

        PerfTrace.getInstance(false, 1001001001001L, 1, 0, true);

        PerfRecord initPerfRecord = new PerfRecord(TGID, 10000001, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord.start();
        Thread.sleep(1050);
        initPerfRecord.end();

        LOG.debug("task writer starts to do prepare ...");
        PerfRecord preparePerfRecord = new PerfRecord(TGID, 10000001, PerfRecord.PHASE.WRITE_TASK_PREPARE);
        preparePerfRecord.start();
        Thread.sleep(1020);
        preparePerfRecord.end();
        LOG.debug("task writer starts to write ...");

        PerfRecord dataPerfRecord = new PerfRecord(TGID, 10000001, PerfRecord.PHASE.READ_TASK_DATA);
        dataPerfRecord.start();

        Thread.sleep(1200);
        dataPerfRecord.addCount(1001);
        dataPerfRecord.addSize(1001);
        dataPerfRecord.end();

        PerfRecord initPerfRecord2 = new PerfRecord(TGID, 10000002, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord2.start();
        Thread.sleep(50);
        initPerfRecord2.end();

        LOG.debug("task writer starts to do prepare ...");
        PerfRecord preparePerfRecord2 = new PerfRecord(TGID, 10000002, PerfRecord.PHASE.WRITE_TASK_PREPARE);
        preparePerfRecord2.start();
        Thread.sleep(20);
        preparePerfRecord2.end();
        LOG.debug("task writer starts to write ...");

        PerfRecord dataPerfRecor2 = new PerfRecord(TGID, 10000002, PerfRecord.PHASE.READ_TASK_DATA);
        dataPerfRecor2.start();

        Thread.sleep(2200);
        dataPerfRecor2.addCount(2001);
        dataPerfRecor2.addSize(2001);
        dataPerfRecor2.end();

        PerfTrace.getInstance().addTaskDetails(10000001,"task 100000011 some thing abcdf");
        PerfTrace.getInstance().addTaskDetails(10000002,"task 100000012 some thing abcdf");
        System.out.println(PerfTrace.getInstance().summarize());
    }
}