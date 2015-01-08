package com.alibaba.datax.plugin.writer.oceanbasewriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.oceanbasewriter.column.ColumnMeta;
import com.alibaba.datax.plugin.writer.oceanbasewriter.column.ColumnMetaFactory;
import com.alibaba.datax.plugin.writer.oceanbasewriter.strategy.Context;
import com.alibaba.datax.plugin.writer.oceanbasewriter.strategy.Strategy;
import com.alibaba.datax.plugin.writer.oceanbasewriter.utils.*;

import java.util.List;

public class OceanbaseWriter extends Writer {
    public static class Job extends Writer.Job{

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return TaskSplitter.split(this.getPluginJobConf(),mandatoryNumber);
        }

        @Override
        public void init() {
            ConfigurationChecker.check(this.getPluginJobConf());
        }

        @Override
        public void prepare() {
            try {
                TaskPrepare.job(this.getPluginJobConf());
            }catch (Exception e){
                throw new RuntimeException("job prepare error",e);
            }
        }

        @Override
        public void destroy() {}
    }

    public static class Task extends Writer.Task{

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            try {
                Context context = new Context(this.getPluginJobConf(), recordReceiver,this.getTaskPluginCollector());
                ActiveMemPercentChecker.launchDaemon(context);
                List<ColumnMeta> columns = ColumnMetaFactory.ColumnMeta(context);
                Strategy strategy = Strategy.instance(context, columns);
                strategy.batchWrite();
            } catch (Exception e) {
                throw new RuntimeException("write ob fail",e);
            }
        }

        @Override
        public void init() {
            try {
                OBDataSource.init(this.getPluginJobConf());
            } catch (Exception e) {
                throw new RuntimeException("init OB datasource error",e);
            }
        }

        @Override
        public void prepare() {
            try {
                TaskPrepare.task(this.getPluginJobConf());
            }catch (Exception e){
                throw new RuntimeException("task prepare error",e);
            }
        }

        @Override
        public void destroy() {
            try {
                OBDataSource.destroy(this.getPluginJobConf());
            } catch (Exception e) {
                throw new RuntimeException("destroy OB datasource error",e);
            }
        }
    }
}
