package com.alibaba.datax.plugin.writer.adswriter;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

/**
 * Created by judy.lt on 2015/2/2.
 */
@RunWith(LoggedRunner.class)
public class AdsWriterTest extends BasicWriterPluginTest {

    @TestLogger(log = "ADS Writer Basic Test")
    @Test
    public void testBasic0() {
//        AdsWriter adsWriter = new AdsWriter();
//        AdsWriter.Job adsWriterJob = new AdsWriter.Job();
//        Configuration jobConf = getJobConf(TESTCLASSES_PATH + File.separator
//                + "basic0.json");
//
//        jobConf.set("jobName", "basic0.json");
//        adsWriterJob.init();
//        adsWriterJob.prepare();
//        adsWriterJob.split(3);
//        adsWriterJob.post();
        int readerSliceNumber = 1;
        super.doWriterTest("basic0.json", readerSliceNumber);
    }

    @Override
    protected List<Record> buildDataForWriter() {
        return null;
    }

    @Override
    protected String getTestPluginName() {
        return null;
    }
}
