package com.alibaba.datax.plugin.writer.otswriter.common;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.plugin.slave.AbstractSlavePluginCollector;
import com.alibaba.datax.core.statistics.metric.Metric;

public class TestPluginCollector extends AbstractSlavePluginCollector {
    
    private List<RecordAndMessage> content = new ArrayList<RecordAndMessage>();

    public class RecordAndMessage {
        private Record dirtyRecord;
        private String errorMessage;
        
        public RecordAndMessage(Record dirtyRecord, String errorMessage) {
            this.dirtyRecord = dirtyRecord;
            this.errorMessage = errorMessage;
        }

        public Record getDirtyRecord() {
            return dirtyRecord;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }

    public TestPluginCollector(Configuration conf, Metric metric,
            PluginType type) {
        super(conf, metric, type);
    }

    @Override
    public void collectDirtyRecord(Record dirtyRecord, Throwable t,
            String errorMessage) {
        content.add(new RecordAndMessage(dirtyRecord, errorMessage));
    }
    
    public List<RecordAndMessage> getContent() {
        return content;
    }
}
