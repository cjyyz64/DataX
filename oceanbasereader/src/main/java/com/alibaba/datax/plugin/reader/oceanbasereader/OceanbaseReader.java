package com.alibaba.datax.plugin.reader.oceanbasereader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.oceanbasereader.command.Context;
import com.alibaba.datax.plugin.reader.oceanbasereader.command.HighVersionCommand;
import com.alibaba.datax.plugin.reader.oceanbasereader.command.LowVersionCommand;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.ConfigurationChecker;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.OBDataSource;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.TaskSplitter;

import java.util.List;

public class OceanbaseReader extends Reader {

    public static class Master extends Reader.Job {

        @Override
        public List<Configuration> split(int adviceNumber) {
            return TaskSplitter.split(super.getPluginJobConf());
        }

        @Override
        public void init() {
            ConfigurationChecker.check(super.getPluginJobConf());
        }

        @Override
        public void destroy() { }
    }

    public static class Slave extends Reader.Task{

        @Override
        public void startRead(RecordSender recordSender) {
            try {
                Context context = new Context(super.getPluginJobConf(),recordSender,super.getTaskPluginCollector());
                if (context.lowVersion()) {
                    new LowVersionCommand().execute(context);
                } else {
                    new HighVersionCommand().execute(context);
                }
            } catch (Throwable e) {
                throw new RuntimeException("read ob fail",e);
            }
        }

        @Override
        public void init() {
            try {
                OBDataSource.init(super.getPluginJobConf());
            }catch (Exception e){
                throw new RuntimeException("init OB datasource error",e);
            }
        }

        @Override
        public void destroy() {
            try {
                OBDataSource.destroy(super.getPluginJobConf());
            }catch (Exception e){
                throw new RuntimeException("destroy OB datasource error",e);
            }
        }
    }
}
