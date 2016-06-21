package com.alibaba.datax.plugin.writer.txtfilewriter;

import java.util.List;

import org.junit.Test;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public class TxtFileWriterTest extends BasicWriterPluginTest {

    @Test
    public void testBasic0() {
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
