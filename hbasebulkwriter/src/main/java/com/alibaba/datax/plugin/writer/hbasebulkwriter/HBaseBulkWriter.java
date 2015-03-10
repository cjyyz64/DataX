package com.alibaba.datax.plugin.writer.hbasebulkwriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseBulkWriter extends Writer {
  
  public static class Job extends Writer.Job {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseBulker.class);


    private Configuration dataxConf;
    HBaseBulker bulker;
    
    private void loadBulker() {
        bulker = new HBaseBulker();
    }
    
    @Override
    public void init() {
      dataxConf = getPluginJobConf();
      loadBulker();
      try {
        bulker.init(dataxConf);
      } catch (IOException e) {
        throw DataXException.asDataXException(BulkWriterError.IO, e);
      }
    }
    

    @Override
    public void prepare() {
      bulker.prepare();
    }

    @Override
    public void destroy() {
      try {
        bulker.finish();
      } catch (Exception e) {
        throw DataXException.asDataXException(BulkWriterError.IO, e);
      }     
    }

    @Override
    public List<Configuration> split(int mandatoryNumber) {
      List<Configuration> slaveConfigurations = new ArrayList<Configuration>();
      for(int i=0;i<mandatoryNumber;i++){
        slaveConfigurations.add(this.dataxConf.clone());
      }

      LOG.info("Writer split to {} parts.", slaveConfigurations.size());

      return slaveConfigurations;
    }
    
    @Override
    public void post() {
        bulker.post();
    }
    
  }
  
  public static class Task extends Writer.Task {
    private Configuration dataxConf;
    HBaseBulker bulker;
    
    private void loadBulker() {
      bulker = new HBaseBulker();
    }
    
    @Override
    public void init() {
      dataxConf = getPluginJobConf();
      loadBulker();
      try {
        bulker.init(dataxConf);
      } catch (IOException e) {
        throw DataXException.asDataXException(BulkWriterError.IO, e);
      }
    }
    
    @Override
    public void destroy() {
      bulker.finish();
    }

    @Override
    public void startWrite(RecordReceiver lineReceiver) {
      bulker.startWrite(new HBaseLineReceiver(lineReceiver)); 
    }
  }
}
