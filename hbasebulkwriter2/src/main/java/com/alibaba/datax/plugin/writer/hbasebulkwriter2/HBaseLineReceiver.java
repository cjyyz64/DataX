package com.alibaba.datax.plugin.writer.hbasebulkwriter2;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;

import java.util.ArrayList;

public class HBaseLineReceiver {
  RecordReceiver receiver;

  public HBaseLineReceiver(RecordReceiver receiver) {
    this.receiver = receiver;
  }

  public ArrayList<Column> read() {
    Record originLine = receiver.getFromReader();
    if (originLine == null) {
      return null;
    }
    ArrayList<Column> line = new ArrayList<Column>(originLine.getColumnNumber());
    for (int i = 0; i < originLine.getColumnNumber(); i++) {
      line.add(originLine.getColumn(i));
    }
    return line;
  }
}