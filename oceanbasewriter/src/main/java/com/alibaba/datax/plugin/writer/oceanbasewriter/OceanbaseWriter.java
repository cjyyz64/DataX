package com.alibaba.datax.plugin.writer.oceanbasewriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.oceanbasewriter.column.ColumnMeta;
import com.alibaba.datax.plugin.writer.oceanbasewriter.column.ColumnMetaFactory;
import com.alibaba.datax.plugin.writer.oceanbasewriter.strategy.Context;
import com.alibaba.datax.plugin.writer.oceanbasewriter.strategy.Strategy;
import com.alibaba.datax.plugin.writer.oceanbasewriter.utils.ActiveMemPercentChecker;
import com.alibaba.datax.plugin.writer.oceanbasewriter.utils.ConfigurationChecker;
import com.alibaba.datax.plugin.writer.oceanbasewriter.utils.OBDataSource;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;

import java.util.List;

public class OceanbaseWriter extends Writer {
    public static class Master extends Writer.Master{

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            Configuration configuration = this.getPluginJobConf();
            List<JSONObject> connections = configuration.getList(Key.CONNECTION, JSONObject.class);
            for (JSONObject connection : connections){
                Configuration slice = Configuration.from(connection);
                String table = slice.getString(Key.TABLE);
                String url = slice.getString(Key.CONFIG_URL);
                configuration.set(Key.TABLE, table);
                configuration.set(Key.CONFIG_URL, url);
                configuration.remove(Key.CONNECTION);
            }
            List<Configuration> configurations = Lists.newArrayList();
            for (int index = 0; index < mandatoryNumber; index++){
                    configurations.add(configuration.clone());
            }
            return configurations;
        }

        @Override
        public void init() {
            ConfigurationChecker.check(this.getPluginJobConf());
        }

        @Override
        public void destroy() { }
    }

    public static class Slave extends Writer.Slave{

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            try {
                Context context = new Context(this.getPluginJobConf(), recordReceiver,this.getSlavePluginCollector());
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
        public void destroy() {
            try {
                OBDataSource.destory();
            } catch (Exception e) {
                throw new RuntimeException("destroy OB datasource error",e);
            }
        }
    }
}
