package com.alibaba.datax.plugin.reader.otsreader.common;

import java.io.OutputStream;
import com.alibaba.datax.test.simulator.BasicReaderPluginTest;

public class TemplateSomketest extends BasicReaderPluginTest{

    
    @Override
    protected OutputStream buildDataOutput(String optionalOutputName) {
        return null;
        //return System.out;
    }

    @Override
    public String getTestPluginName() {
        return "otsreader";
    }
}
