package com.alibaba.datax.plugin.writer.otswriter.common;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.plugin.slave.AbstractSlavePluginCollector;
import com.alibaba.datax.core.statistics.communication.Communication;

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

    public TestPluginCollector(Configuration conf, Communication communication,
            PluginType type) {
        super(conf, communication, type);
    }

    @Override
    public void collectDirtyRecord(Record dirtyRecord, Throwable t,
            String errorMessage) {
        content.add(new RecordAndMessage(dirtyRecord, errorMessage));
    }
    
    public List<RecordAndMessage> getContent() {
        return content;
    }
    
    public List<Record> getRecord() {
        List<Record> records = new ArrayList<Record>();
        for (RecordAndMessage rm : content) {
            records.add(rm.getDirtyRecord());
        }
        return records;
    }
}
