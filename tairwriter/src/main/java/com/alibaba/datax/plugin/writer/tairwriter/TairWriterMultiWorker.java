package com.alibaba.datax.plugin.writer.tairwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairManager;
import com.taobao.tair.etc.KeyCountPack;
import com.taobao.tair.etc.KeyValuePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class TairWriterMultiWorker extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(TairWriterMultiWorker.class);
    private static final boolean IS_DEBUG = LOG.isDebugEnabled();

    private TairManager tm = null;
    private TaskPluginCollector collector = null;
    private TairConfig conf = null;

    private static final int MAX_RETRY_TIMES = 600;

    private Serializable key;
    private StringBuilder value;
    private List<KeyValuePack> keyValuePacks = new ArrayList<KeyValuePack>();
    private List<KeyCountPack> keyCountPacks = new ArrayList<KeyCountPack>();
    private int expire;
    private final int threadId;
    private final ArrayBlockingQueue<Record> workQueue;
    private final AtomicReference<Exception> threadException;
    private volatile boolean isShutdown = false;
    private long records = 0L;

    TairWriterMultiWorker(int threadId, TairManager tm, TairConfig conf, TaskPluginCollector collector
            , int queueSize, AtomicReference<Exception> threadException) {
        super(String.format("TairWriterMultiWorker[%s]", threadId));
        this.tm = tm;
        this.conf = conf;
        this.collector = collector;
        this.expire = conf.getExpire();
        this.value = new StringBuilder();
        this.threadId = threadId;
        this.workQueue = new ArrayBlockingQueue<Record>(queueSize);
        this.threadException = threadException;
    }

    @Override
    public void run() {
        LOG.info(String.format("TairWriterMultiWorker[%s] write start.", threadId));

        Record record = null;

        long lastTime = System.currentTimeMillis();
        while (!isShutdown || workQueue.size()>0) {
            try {
                record = workQueue.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.error("workQueue.poll has Exception: " + e.getMessage(), e);
            }

            long currentTime = System.currentTimeMillis();
            //每分钟打印一次queue的size，作为调整并发的依据
            if (currentTime > lastTime + 120000) {
                LOG.info(String.format("TairWriterMultiWorker[%s] NOW write records=%s ...", threadId,records));
                lastTime = currentTime;
            }

            if (record == null) {
                continue;
            }
            try {
                write(record);
            } catch (Exception e) {
                //这个地方异常，只有可能是记录脏数据时，因为插件不支持导致的异常
                LOG.error("TairWriterMultiWorker.write has Exception: " + e.getMessage(), e);
                threadException.set(e);
                break;
            }
        }

        LOG.info(String.format("TairWriterMultiWorker[%s] write finished. total records=%s", threadId,records));
    }

    public void setIsShutdown(boolean isShutdown) {
        this.isShutdown = isShutdown;
    }

    int write(Record record) {

        try {
            if ("put".equalsIgnoreCase(conf.getWriterType())) {
                if (!checkColumnValid(record, 2))
                    return 0;
                put(record);
            } else if ("counter".equalsIgnoreCase(conf.getWriterType())) {
                if (!checkColumnValid(record, 2))
                    return 0;
                setCount(record);
            } else if ("prefixput".equalsIgnoreCase(conf.getWriterType())) {
                if (!checkColumnValid(record, 3))
                    return 0;
                prefixPut(record);
            } else if ("prefixcounter".equalsIgnoreCase(conf.getWriterType())) {
                if (!checkColumnValid(record, 3))
                    return 0;
                prefixSetCount(record);
            } else if ("multiprefixput".equalsIgnoreCase(conf.getWriterType())) {
                if (!checkColumnValid(record, 2))
                    return 0;
                multiPrefixPuts(record);
            } else if ("multiprefixcounter".equalsIgnoreCase(conf.getWriterType())) {
                if (!checkColumnValid(record, 2))
                    return 0;
                multiPrefixSetCounts(record);
            }

        } catch (Exception e) {
            collector.collectDirtyRecord(record, " This Record throw Exception: " + e.getMessage());
        }
        return 0;
    }

    boolean checkColumnValid(Record record, int minColumnNum) {

        int fieldNum = record.getColumnNumber();
        if (fieldNum < minColumnNum) {
            collector.collectDirtyRecord(record, "filed number must >= " + minColumnNum);
            return false;
        }
        for (int i = 0; i < fieldNum; ++i) {
            if (record.getColumn(i) == null)
                return false;
        }

        if (null == record.getColumn(0).getRawData()) {
            collector.collectDirtyRecord(record, "key不能为空");
            return false;
        }
        return true;
    }



    private int put(Record record) throws Exception {

        key = getKeyFromColumn(conf,record);
        String v = convertColumnToValue(record);
        if (null != v) {
            String errorInfo = retryPut(key, v, expire, record);
            if (null != errorInfo) {
                LOG.warn("put fail, record: " + record);
                collector.collectDirtyRecord(record, errorInfo + ", key:"
                        + key + "value: " + value);
                return -1;
            }
        } else if (conf.isDeleteEmptyRecord()) {
            ResultCode rc = tm.delete(conf.getNamespace(), key);
            if (ResultCode.SUCCESS.getCode() != rc.getCode()
                    && ResultCode.DATANOTEXSITS.getCode() != rc.getCode()) {
                LOG.warn("delete record:" + record.toString() + " fail, rc: " + rc.getCode());
                collector.collectDirtyRecord(record, " delete empty record fail");
                return -1;
            }
        } else {
            collector.collectDirtyRecord(record, "record all value fields are null");
            return -1;
        }
        return 0;
    }

    private int prefixPut(Record record) throws Exception {
        int fieldNum = record.getColumnNumber();
        if (fieldNum != 3) {
            collector.collectDirtyRecord(record, "prefixput record field number must be 3");
            return -1;
        }

        if (null == record.getColumn(1).getRawData() ||
                null == record.getColumn(2).getRawData()) {
            collector.collectDirtyRecord(record, "skey and value 不能为空");
            return -1;
        }

        keyValuePacks.clear();
        key = getKeyFromColumn(conf,record);
        Serializable skey = record.getColumn(1).asString();
        Serializable svalue = record.getColumn(2).asString();
        KeyValuePack pack = new KeyValuePack(skey, svalue, (short) 0, expire);
        keyValuePacks.add(pack);

        ResultCode rc = tm.prefixPuts(conf.getNamespace(), key, keyValuePacks).getRc();
        if (ResultCode.SUCCESS.getCode() != rc.getCode()) {
            String errorInfo = getTairErrorInfo(rc);
            collector.collectDirtyRecord(record, "prefixput fail: " + errorInfo);
            return -1;
        }
        return 0;
    }

    private int multiPrefixPuts(Record record) throws Exception {
        int fieldNum = record.getColumnNumber();
        if (fieldNum != 1 + conf.getSkeyList().size()) {
            collector.collectDirtyRecord(record, "multiprefixput record field number must equal skey number + 1");
            return -1;
        }

        keyValuePacks.clear();
        key = getKeyFromColumn(conf,record);
        // i is index of record data column and column name
        for (int i = 1; i < record.getColumnNumber(); ++i) {
            if (null != record.getColumn(i).getRawData()) {
                KeyValuePack pack = new KeyValuePack(conf.getSkeyList().get(i - 1),
                        record.getColumn(i).asString(), (short) 0, expire);
                keyValuePacks.add(pack);
            }
        }
        if (keyValuePacks.size() == 0) {
            LOG.warn("pkey:" + key + " value column empty");
            collector.collectDirtyRecord(record, "value不能为空");
            return -1;
        }
        ResultCode rc = tm.prefixPuts(conf.getNamespace(), key, keyValuePacks).getRc();
        if (ResultCode.SUCCESS.getCode() != rc.getCode()) {
            String errorInfo = getTairErrorInfo(rc);
            collector.collectDirtyRecord(record, "multiprefixput fail: " + errorInfo);
            return -1;
        }
        return 0;
    }

    private int setCount(Record record) throws Exception {
        int fieldNum = record.getColumnNumber();
        if (fieldNum != 2) {
            collector.collectDirtyRecord(record, "counter 必须是2个字段");
            return -1;
        }
        key = getKeyFromColumn(conf,record);
        if (null == record.getColumn(1).getRawData()) {
            collector.collectDirtyRecord(record, "count不能为空");
            return -1;
        }

        Long count = record.getColumn(1).asLong();
        if (count < 0 || count > Integer.MAX_VALUE) {
            collector.collectDirtyRecord(record, "数字超出范围 [0, Int_max]");
            return -1;
        }
        String errorInfo = retrySetCount(key, count, expire, record);
        if (null != errorInfo) {
            LOG.warn("counter fail, record: " + record);
            collector.collectDirtyRecord(record, errorInfo + ", key:"
                    + key + "value: " + count);
            return -1;
        }
        return 0;
    }

    private int prefixSetCount(Record record) throws Exception {
        int fieldNum = record.getColumnNumber();
        if (fieldNum != 3) {
            collector.collectDirtyRecord(record, "prefixcounter record field number must be 3");
            return -1;
        }

        if (null == record.getColumn(1).getRawData() ||
                null == record.getColumn(2).getRawData()) {
            collector.collectDirtyRecord(record, "skey and count 不能为空");
            return -1;
        }

        keyValuePacks.clear();
        key = getKeyFromColumn(conf,record);
        Serializable skey = record.getColumn(1).asString();
        Long count = record.getColumn(2).asLong();
        if (count >= 0 && count <= Integer.MAX_VALUE) {
            KeyCountPack pack = new KeyCountPack(skey, count.intValue(), (short) 0, expire);
            keyCountPacks.add(pack);
        } else {
            collector.collectDirtyRecord(record, "数字超出范围 [0, Int_max]");
            return -1;
        }

        ResultCode rc = tm.prefixSetCounts(conf.getNamespace(), key, keyCountPacks).getRc();
        if (ResultCode.SUCCESS.getCode() != rc.getCode()) {
            String errorInfo = getTairErrorInfo(rc);
            collector.collectDirtyRecord(record, "prefixcounter fail: " + errorInfo);
            return -1;
        }
        return 0;
    }

    public static String getKeyFromColumn(TairConfig conf, Record record) {
        String columnKey = "";
        if (null != conf.getFrontLeadingKey()) {
            columnKey += conf.getFrontLeadingKey();
        }
        columnKey += record.getColumn(0).asString();
        return columnKey;
    }

    private int multiPrefixSetCounts(Record record) throws Exception {
        int fieldNum = record.getColumnNumber();
        if (fieldNum != 1 + conf.getSkeyList().size()) {
            collector.collectDirtyRecord(record, "multiprefixcounter record field number must equal skey number + 1");
            return -1;
        }

        keyCountPacks.clear();
        key = getKeyFromColumn(conf,record);
        for (int i = 1; i < record.getColumnNumber(); ++i) {
            if (null != record.getColumn(i).getRawData()) {
                Long count = record.getColumn(i).asLong();
                if (count >= 0 && count <= Integer.MAX_VALUE) {
                    KeyCountPack pack = new KeyCountPack(conf.getSkeyList().get(i - 1),
                            count.intValue(), (short) 0, expire);
                    keyCountPacks.add(pack);
                } else {
                    collector.collectDirtyRecord(record, "数字超出范围 [0, Int_max]");
                }
            }
        }
        if (keyCountPacks.size() == 0) {
            LOG.warn("pkey:" + key + " skey is empty");
            collector.collectDirtyRecord(record, "value不能为空");
            return -1;
        }

        ResultCode rc = tm.prefixSetCounts(conf.getNamespace(), key, keyCountPacks).getRc();
        if (ResultCode.SUCCESS.getCode() != rc.getCode()) {
            String errorInfo = getTairErrorInfo(rc);
            collector.collectDirtyRecord(record, "multiprefixcounter fail: " + errorInfo);
            return -1;
        }
        return 0;
    }

    private String getTairErrorInfo(ResultCode rc) {

        if (rc.getCode() == ResultCode.KEYTOLARGE.getCode()) {
            return new String("Key超过1KB");
        } else if (rc.getCode() == ResultCode.SERIALIZEERROR.getCode()) {
            return new String("KV序列化错误");
        } else if (rc.getCode() == ResultCode.VALUETOLARGE.getCode()) {
            return new String("Value超过1MB");
        } else if (ResultCode.TIMEOUT.getCode() == rc.getCode()
                || ResultCode.CONNERROR.getCode() == rc.getCode()) {
            return new String("tair操作超时");
        } else {
            return new String("tair集群操作失败， rc: " + rc.getCode());
        }
    }

    private String retryPut(Serializable key, String value, int expire, Record record) throws Exception {
        int cnt = 0;
        while (cnt <= MAX_RETRY_TIMES) {
            ResultCode rc = tm.put(conf.getNamespace(), key, value, 0, expire);
            if (ResultCode.SUCCESS.getCode() == rc.getCode()) {
                return null;
            } else if (ResultCode.TIMEOUT.getCode() == rc.getCode()
                    || ResultCode.CONNERROR.getCode() == rc.getCode()) {
                LOG.warn("operator record:" + record + " timeout rc:" + rc.getCode());
                if (doCheck(key, value)) {
                    return null;// success actually
                }
                ++cnt;
            } else if (ResultCode.OVERFLOW.getCode() == rc.getCode()
                    || ResultCode.SERVERERROR.getCode() == rc.getCode()) {
                Thread.sleep(1);
                LOG.error(String.format(
                        "Tair server rejected error(overflow or server exception), return code [%s] .", rc));
            } else {
                return getTairErrorInfo(rc);
            }
        }
        return new String("timeout retry put fail finally");
    }

    // return error info
    private String retrySetCount(Serializable key, Long count, int expire, Record record) throws Exception {
        int cnt = 0;
        while (cnt <= MAX_RETRY_TIMES) {
            ResultCode rc = tm.setCount(conf.getNamespace(), key, count.intValue(), 0, expire);
            if (ResultCode.SUCCESS.getCode() == rc.getCode()) {
                return null;
            } else if (ResultCode.TIMEOUT.getCode() == rc.getCode()
                    || ResultCode.CONNERROR.getCode() == rc.getCode()) {
                LOG.warn("operator key:" + record + " timeout rc:" + rc.getCode());
                if (doCheck(key, count.toString())) {
                    return null;// success actually
                }
                ++cnt;
            } else if (ResultCode.OVERFLOW.getCode() == rc.getCode()
                    || ResultCode.SERVERERROR.getCode() == rc.getCode()) {
                Thread.sleep(1);
                LOG.error(String.format(
                        "Tair server rejected error(overflow or server exception), return code [%s] .", rc));
            } else {
                return getTairErrorInfo(rc);
            }
        }
        return new String("timeout retry setCount fail finally");
    }

    private boolean doCheck(Serializable key, String value) {
        Result<DataEntry> rcd = null;
        if (null == value) {
            return false;
        }
        rcd = tm.get(conf.getNamespace(), key);
        if (rcd.getRc().getCode() == ResultCode.SUCCESS.getCode()) {
            if (rcd.getValue().getValue().equals(value)) {
                return true;
            }
        }
        return false;
    }

    /*
    private Serializable convertColumn(Column column) {
        try {
            if ("c++".equalsIgnoreCase(conf.getLanguage())) {
                return column.asString();
            } else {
                switch (column.getType()) {
                case STRING:
                    return column.asString();
                case LONG:
                    return column.asLong();
                case DOUBLE:
                    return column.asDouble();
                case BOOL:
                    return column.asBoolean();
                case DATE:
                    return column.asDate();
                case BYTES:
                    return column.asBytes();
                case NULL:
                    return null;
                default:
                    throw new IllegalArgumentException("Convert Column to Key/Value failed");
                }
            }
        } catch (DataXException e) {
             throw new IllegalArgumentException("Convert Column to Key/Value failed");
        }
    }*/


    private String convertColumnToValue(Record record) {
        int fieldNum = record.getColumnNumber();
        if (fieldNum > 2) {
            // value all convert to string
            value.delete(0, value.length());
            boolean allIsNull = true;
            for (int i = 1; i < fieldNum; ++i) {
                if (null != record.getColumn(i).getRawData()) {
                    String fieldString = record.getColumn(i).asString();
                    value.append(fieldString);
                    allIsNull = false;
                } else {
                    // null string
                }
                value.append(conf.getFieldDelimiter());
            }
            if (!allIsNull) {
                value.setLength(value.length() - 1);
                return value.toString();
            } else {
                return null;
            }
        } else if (fieldNum == 2) {
            return record.getColumn(1).asString();
        } else {
            return null;
        }
    }


    public void send(Record record) {
        try {
            workQueue.put(record);
            records++;
        } catch (InterruptedException e) {
            LOG.error("TairWriterMultiWorker.send has Exception: " + e.getMessage(), e);
            threadException.set(e);
        }
    }
}
