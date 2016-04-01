package com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x.column.DynamicHBaseColumn;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x.column.FixedHBaseColumn;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x.column.HBaseColumn;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
@SuppressWarnings("Duplicates")
public class WritePolicer {

    public static Comparator<KeyValue> KEY_VALUE_COMPARATOR = new Comparator<KeyValue>() {
        @Override
        public int compare(KeyValue o1, KeyValue o2) {
            /**
             * 默认值也是这个,其实可以去掉
             */
            return KeyValue.COMPARATOR.compare(o1, o2);
        }
    };

    public static void appendKVsList(HFileWriter writer,
                                     LinkedList<KeyValue[]> kvsList) throws IOException {
        if (kvsList.size() == 1) {
            KeyValue[] kvs = kvsList.getFirst();
            for (KeyValue kv : kvs) {
                writer.write(null, kv);
            }
        } else {
            /**
             * The key structure in HFile: | rowkey | family:qualifier |
             * timestamp | type |. Because the keys are ascending order
             * according to the family:qualifier name, the same column should be
             * added together when the rowkey of them are equal.
             */
            for (int i = 0, l = kvsList.size() != 0 ? kvsList.getFirst().length
                    : 0; i < l; i++) {
                for (KeyValue[] kvs : kvsList) {
                    writer.write(null, kvs[i]);
                }
            }
        }
    }

    public static void appendKVsQueue(HFileWriter writer,
                                      PriorityQueue<KeyValue> kvsQueue) throws IOException {
        int l = kvsQueue.size();
        for (int i = 0; i < l; i++) {
            KeyValue kv = kvsQueue.poll();
            writer.write(null, kv);
        }
    }

    public static void fixed(HFileWriter writer, HBaseLineReceiver receiver,
                             int bucketNum, List<FixedHBaseColumn> rowkeyList,
                             List<FixedHBaseColumn> columnList, String encoding, int timeCol,
                             long startTs, String nullMode, TaskPluginCollector taskPluginCollector) throws IllegalAccessException, IOException,
            ParseException {

        if (timeCol != -1 && startTs != -1) {
            throw new IllegalArgumentException(
                    "can not set time_col and start_ts at the same time");
        }

        if (timeCol == -1) {
            parseTimestampOrderedLine(writer, receiver, bucketNum, rowkeyList,
                    columnList, encoding, startTs, nullMode, taskPluginCollector);
        } else {
            parseTimestampSpecifiedLine(writer, receiver, bucketNum,
                    rowkeyList, columnList, encoding, timeCol, nullMode, taskPluginCollector);
        }
    }

    private static void parseTimestampSpecifiedLine(HFileWriter writer,
                                                    HBaseLineReceiver receiver, int bucketNum, List<FixedHBaseColumn> rowkeyList,
                                                    List<FixedHBaseColumn> columnList, String encoding, int timeCol,
                                                    String nullMode, TaskPluginCollector taskPluginCollector) throws IOException, ParseException {
        HBaseLineReceiver.HBaseRecord hBaseRecord;
        ArrayList<Column> line;
        byte[] preRow = null;
        PriorityQueue<KeyValue> kvsQueue = new PriorityQueue<KeyValue>(200,
                KEY_VALUE_COMPARATOR);
        SimpleDateFormat df_senconds = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat df_ms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
        while ((hBaseRecord = receiver.read()) != null) {
            try {
                line = hBaseRecord.getLine();
                byte[] rowkey = FixedHBaseColumn.toRow(line, bucketNum,
                        rowkeyList);
                if (isDifferentRow(preRow, rowkey)) {
                    appendKVsQueue(writer, kvsQueue);
                    kvsQueue = new PriorityQueue<KeyValue>(200,
                            KEY_VALUE_COMPARATOR);
                }
                long timestamp;
                if(line.get(timeCol) instanceof LongColumn || line.get(timeCol) instanceof DoubleColumn){
                    timestamp = line.get(timeCol).asLong();
                }else {
                    Date date;

                    try{
                        date = df_ms.parse(line.get(timeCol).asString());
                    }catch (ParseException e){
                        date = df_senconds.parse(line.get(timeCol).asString());
                    }
                    timestamp = date.getTime();
                }
                KeyValue[] curKvs = FixedHBaseColumn.toKVs(line, rowkey,
                        columnList, encoding, timestamp, nullMode);
                for (KeyValue kv : curKvs) {
                    if(kv != null) {
                        kvsQueue.add(kv);
                    }
                }
                preRow = rowkey;
            } catch (SQLException e) {
                taskPluginCollector.collectDirtyRecord(hBaseRecord.getRecord(), e);
            }
        }
        appendKVsQueue(writer, kvsQueue);
    }

    private static void parseTimestampOrderedLine(HFileWriter writer,
                                                  HBaseLineReceiver receiver, int bucketNum, List<FixedHBaseColumn> rowkeyList,
                                                  List<FixedHBaseColumn> columnList, String encoding, long startTs,
                                                  String nullMode, TaskPluginCollector taskPluginCollector) throws IOException {
        HBaseLineReceiver.HBaseRecord hBaseRecord;
        ArrayList<Column> line;
        byte[] preRow = null;
        LinkedList<KeyValue[]> kvsList = new LinkedList<KeyValue[]>();
        long timestamp = startTs != -1 ? startTs : System
                .currentTimeMillis();
        while ((hBaseRecord = receiver.read()) != null) {

            try {
                line = hBaseRecord.getLine();
                byte[] rowkey = FixedHBaseColumn.toRow(line, bucketNum,
                        rowkeyList);
                if (isDifferentRow(preRow, rowkey)) {
                    appendKVsList(writer, kvsList);
                    kvsList = new LinkedList<KeyValue[]>();
                    timestamp = startTs != -1 ? startTs : System
                            .currentTimeMillis();
                }
                /**
                 * The key structure in HFile: | rowkey | family:qualifier |
                 * timestamp | type |. Because the keys are descending order
                 * according to the timestamp, the latest KeyValue should be
                 * added firstly when the rowkey of them are equal.
                 */
                KeyValue[] curKvs = FixedHBaseColumn.toKVs(line, rowkey,
                        columnList, encoding, timestamp, nullMode);
                kvsList.addFirst(curKvs);
                preRow = rowkey;
                timestamp++;
            } catch (SQLException e) {
                taskPluginCollector.collectDirtyRecord(hBaseRecord.getRecord(), e);
            }
        }
        appendKVsList(writer, kvsList);
    }

    private static boolean isDifferentRow(byte[] preRow, byte[] rowkey) {
        int comp = preRow == null ? 0 : Bytes.BYTES_RAWCOMPARATOR.compare(
                preRow, 0, preRow.length, rowkey, 0, rowkey.length);
        return comp != 0;
    }

    public static void dynamic(HFileWriter writer, HBaseLineReceiver receiver,
                               HBaseColumn.HBaseDataType rowkeyType, List<DynamicHBaseColumn> columnList, TaskPluginCollector taskPluginCollector)
            throws IOException {
        ArrayList<Column> line;
        HBaseLineReceiver.HBaseRecord hBaseRecord;
        while ((hBaseRecord = receiver.read()) != null) {

            try {
                line = hBaseRecord.getLine();
                writer.write(null,
                        DynamicHBaseColumn.toKV(line, rowkeyType, columnList));
            } catch (SQLException e) {
                taskPluginCollector.collectDirtyRecord(hBaseRecord.getRecord(), e);
            }
        }
    }

}
