package com.alibaba.datax.plugin.reader.otsreader;

import com.alibaba.datax.common.util.Configuration;

import java.util.List;

public interface IOtsReaderMasterProxy {

    public void init(Configuration param) throws Exception;

    public List<Configuration> split(int num) throws Exception;

    public void close();

}
