package com.alibaba.datax.plugin.writer.otswriter.sample;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.plugin.writer.otswriter.common.BaseTest;
import com.alibaba.datax.plugin.writer.otswriter.common.Table;
import com.aliyun.openservices.ots.model.PrimaryKeyType;

public class TestForDatax {

    public static void main(String[] args) throws Exception {
        String t0 = "ots_writer_t0";
        String t1 = "ots_writer_t1";
        BaseTest base = new BaseTest(t0);
        
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        pk.add(PrimaryKeyType.INTEGER);
        
        base.prepareData(pk, 0, 10000, 0);
        
        Table t = new Table(base.getOts(), t1, pk, null, 0);
        t.create(5000, 5000);
        
        base.getOts().shutdown();
    }
    
}
