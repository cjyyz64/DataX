package com.alibaba.datax.plugin.writer.hbasebulkwriter2;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;

import java.util.ArrayList;

public class HBaseLineReceiver {

    public static class HBaseRecord {
        private final Record record;
        private final ArrayList<Column> line;

        public HBaseRecord(Record record, ArrayList<Column> line) {
            this.record = record;
            this.line = line;
        }

        public Record getRecord() {
            return record;
        }

        public ArrayList<Column> getLine() {
            return line;
        }


    }

    RecordReceiver receiver;

    public HBaseLineReceiver(RecordReceiver receiver) {
        this.receiver = receiver;
    }

    public HBaseRecord read() {
        Record originLine = receiver.getFromReader();
        if (originLine == null) {
            return null;
        }
        ArrayList<Column> line = new ArrayList<Column>(originLine.getColumnNumber());
        for (int i = 0; i < originLine.getColumnNumber(); i++) {
            line.add(originLine.getColumn(i));
        }
        return new HBaseRecord(originLine, line);
    }
}