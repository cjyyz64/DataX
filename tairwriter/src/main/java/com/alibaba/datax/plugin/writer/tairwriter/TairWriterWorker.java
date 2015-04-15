package com.alibaba.datax.plugin.writer.tairwriter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairManager;
import com.taobao.tair.etc.KeyCountPack;
import com.taobao.tair.etc.KeyValuePack;

class TairWriterWorker {
    private static final Logger LOG = LoggerFactory.getLogger(TairWriterWorker.class);
    private static final boolean IS_DEBUG = LOG.isDebugEnabled();

    private TairManager tm = null;
    private TaskPluginCollector collector = null;
    private TairConfig conf = null;

    private static final int MAX_RETRY_TIMES = 10;

    private Serializable key;
    private StringBuilder value;
    private List<KeyValuePack> keyValuePacks = new ArrayList<KeyValuePack> ();
    private List<KeyCountPack> keyCountPacks = new ArrayList<KeyCountPack> ();
    private int expire;

    TairWriterWorker(TairManager tm,  TairConfig conf, TaskPluginCollector collector) {
        this.tm = tm;
        this.conf = conf;
        this.collector = collector;
        this.expire = conf.getExpire();
        this.value = new StringBuilder();
    }

    int write(RecordReceiver recordReceiver) throws Exception {
        Record record = null;

        if ("put".equalsIgnoreCase(conf.getWriterType())) {
            while ((record = recordReceiver.getFromReader()) != null) {
                put(record);
            }
        } else if ("counter".equalsIgnoreCase(conf.getWriterType())) {
            while ((record = recordReceiver.getFromReader()) != null) {
                setCount(record);
            }
        } else if ("prefixput".equalsIgnoreCase(conf.getWriterType())) {
            while ((record = recordReceiver.getFromReader()) != null) {
                prefixPut(record);
            }
        } else if ("prefixcounter".equalsIgnoreCase(conf.getWriterType())) {
            while ((record = recordReceiver.getFromReader()) != null) {
                prefixSetCount(record);
            }
        } else if ("multiprefixput".equalsIgnoreCase(conf.getWriterType())) {
            while ((record = recordReceiver.getFromReader()) != null) {
                multiPrefixPuts(record);
            }
        } else if ("multiprefixcounter".equalsIgnoreCase(conf.getWriterType())) {
            while ((record = recordReceiver.getFromReader()) != null) {
                multiPrefixSetCounts(record);
            }
        }

        return 0;
    }

    //TODO: log reduce
    private int put(Record record) throws Exception {
        int fieldNum = record.getColumnNumber();
        if (fieldNum <= 1) {
            collector.collectDirtyRecord(record, "filed number must >= 2");
            return -1;
        }
        key = record.getColumn(0).asString();
        if (null == key) {
            collector.collectDirtyRecord(record, "key can't be null");
            return -1;
        }

        String v = convertColumnToValue(record);
        if (null != v) {
            retryPut(key, v, expire, record);
        } else if (conf.isDeleteEmptyRecord()) {
            ResultCode rc = tm.delete(conf.getNamespace(), key);
            if (ResultCode.SUCCESS.getCode() != rc.getCode()) {
                LOG.warn("delete record:" + record.toString() +  " fail, rc: " + rc.getCode());
                collector.collectDirtyRecord(record, " delete empty record fail");
            }

        } else {
            collector.collectDirtyRecord(record, "record all fields(exclude key) are null");
            return -1;
        }
        return 0;
    }

    private int prefixPut(Record record) throws Exception {
        int fieldNum = record.getColumnNumber();
        if (fieldNum != 3) {
            collector.collectDirtyRecord(record, "prefixPut record field number must be 3");
            return -1;
        }

        keyValuePacks.clear();
        key = record.getColumn(0).asString();
        if (null == key) {
            collector.collectDirtyRecord(record, "key不能为空");
            return -1;
        }
        if (null == record.getColumn(1).asString() ||
                null == record.getColumn(2).asString()) {
            collector.collectDirtyRecord(record, "skey value 不能为空");
            return -1;
        }

        Serializable skey = record.getColumn(1).asString();
        Serializable svalue = record.getColumn(2).asString();
        KeyValuePack pack = new KeyValuePack(skey, svalue ,(short) 0, expire);
        keyValuePacks.add(pack);

        ResultCode rc = tm.prefixPuts(conf.getNamespace(), key, keyValuePacks).getRc();
        boolean isRetry = checkResultCode(rc, record);
        if (isRetry) {
            collector.collectDirtyRecord(record, "prefixPut 超时");
            return -1;
        }
        return 0;
    }

    private int multiPrefixPuts(Record record) throws Exception {
        int fieldNum = record.getColumnNumber();
        if (fieldNum < 2 || fieldNum != 1 + conf.getSkeyList().size()) {
            collector.collectDirtyRecord(record, "multiprefixPuts record field number must >= 2 and equal skey number + 1");
            return -1;
        }

        keyValuePacks.clear();
        key = record.getColumn(0).asString();
        if (null == key) {
            collector.collectDirtyRecord(record, "key不能为空");
            return -1;
        }

        // i is index of record data column and column name
        for (int i = 1; i < record.getColumnNumber(); ++i) {
            if (null != record.getColumn(i).asString()) {
                KeyValuePack pack = new KeyValuePack(conf.getSkeyList().get(i - 1),
                        record.getColumn(i).asString() ,(short) 0, expire);
                keyValuePacks.add(pack);
            }
        }
        if (keyValuePacks.size() == 0) {
            LOG.warn("pkey:" + key + " value column empty");
            collector.collectDirtyRecord(record, "value不能为空");
            return -1;
        }
        ResultCode rc = tm.prefixPuts(conf.getNamespace(), key, keyValuePacks).getRc();
        boolean isRetry = checkResultCode(rc, record);
        if (isRetry) {
            collector.collectDirtyRecord(record, "prefixPut 超时");
            return -1;
        }
        return 0;
    }

    private int setCount(Record record) throws Exception {
        int fieldNum = record.getColumnNumber();
        if (fieldNum != 2) {
            collector.collectDirtyRecord(record, "setCount必须是2个字段");
            return -1;
        }
        key = record.getColumn(0).asString();
        if (null == key) {
            collector.collectDirtyRecord(record, "key不能为空");
            return -1;
        }
        if (null == record.getColumn(1).getRawData()) {//TODO: all modify
          collector.collectDirtyRecord(record, "count不能为空");
          return -1;
        }

        int count = record.getColumn(1).asLong().intValue();
        if (count < 0 || count > Integer.MAX_VALUE) {
            collector.collectDirtyRecord(record, "数字超出范围 [0, Int_max]");
            return -1;
        }
        retrySetCount(key, count, expire, record);
        return 0;
    }

    private int prefixSetCount(Record record) throws Exception {
        int fieldNum = record.getColumnNumber();
        if (fieldNum != 3) {
            collector.collectDirtyRecord(record, "prefixcounter record field number must be 3");
            return -1;
        }

        keyValuePacks.clear();
        key = record.getColumn(0).asString();
        if (null == key) {
            collector.collectDirtyRecord(record, "key不能为空");
            return -1;
        }
        if (null == record.getColumn(1).asString() ||
                null == record.getColumn(2).asString()) {
            collector.collectDirtyRecord(record, "skey count 不能为空");
            return -1;
        }

        Serializable skey = record.getColumn(1).asString();
        int count = record.getColumn(2).asLong().intValue();//TODO: overflow
        if (count >= 0 && count <= Integer.MAX_VALUE) {
            KeyCountPack pack = new KeyCountPack(skey, count ,(short) 0, expire);
            keyCountPacks.add(pack);
        } else {
            collector.collectDirtyRecord(record, "数字超出范围 [0, Int_max]");
            return -1;
        }

        ResultCode rc = tm.prefixSetCounts(conf.getNamespace(), key, keyCountPacks).getRc();
        boolean isRetry = checkResultCode(rc, record);
        if (isRetry) {
            collector.collectDirtyRecord(record, "prefixPut 超时");
            return -1;
        }
        return 0;
    }

    private int multiPrefixSetCounts(Record record) throws Exception {
        int fieldNum = record.getColumnNumber();
        if (fieldNum < 2 || fieldNum != 1 + conf.getSkeyList().size()) {
            collector.collectDirtyRecord(record, "multiprefixcounter record field number must >= 2 and equal skey number + 1");
            return -1;
        }
        keyCountPacks.clear();
        key = record.getColumn(0).asString();
        if (null == key) {
            collector.collectDirtyRecord(record, "key不能为空");
            return -1;
        }

        for (int i = 1; i < record.getColumnNumber(); ++i) {
            if (null != record.getColumn(i).asString()) {
                int count = record.getColumn(i).asLong().intValue();
                if (count >= 0) {
                    KeyCountPack pack = new KeyCountPack(conf.getSkeyList().get(i - 1),
                            count, (short) 0, expire);
                    keyCountPacks.add(pack);
                } else {
                    collector.collectDirtyRecord(record, "数字超出范围 [0, Int_max]");
                }
            } // null string
        }
        if (keyCountPacks.size() == 0) {
            LOG.warn("pkey:" + key + " skey is empty");
            collector.collectDirtyRecord(record, "value不能为空");
            return -1;
        }

        ResultCode rc = tm.prefixSetCounts(conf.getNamespace(), key, keyCountPacks).getRc();
        boolean isRetry = checkResultCode(rc, record);
        if (isRetry) {
            collector.collectDirtyRecord(record, "prefixPut 超时");
            return -1;
        }
        return 0;
    }

    // will need retry is return true
    // TODO: fun name
    private boolean checkResultCode(ResultCode rc, Record record) {
        if (ResultCode.SUCCESS.getCode() == rc.getCode()) {
            return false;
        } else if (ResultCode.TIMEOUT.getCode() == rc.getCode() || ResultCode.CONNERROR.getCode() == rc.getCode()) {
            LOG.warn("operator key:" + record + " timeout rc:" + rc.getCode());
            return true;
        } else if (rc.getCode() == ResultCode.KEYTOLARGE.getCode()) {
            collector.collectDirtyRecord(record, "Key超过1KB");
            return false;
        } else if (rc.getCode() == ResultCode.SERIALIZEERROR.getCode()) {
            collector.collectDirtyRecord(record, "KV序列化错误");
            return false;
        } else if (rc.getCode() == ResultCode.VALUETOLARGE.getCode()) {
            collector.collectDirtyRecord(record, "Value超过1MB");
            return false;
        } else {
            collector.collectDirtyRecord(record, "tair集群操作失败， rc: " + rc.getCode());
            return false;
        }
    }

    // TODO: Object
    // TODO: dirty move out
    private int retryPut(Serializable key, Object value, int expire, Record record)
    {
        ResultCode rc = ResultCode.SERVERERROR;
        boolean isRetry = true;
        int count = -1;
        while (isRetry && ++count < MAX_RETRY_TIMES) {
            rc = tm.put(conf.getNamespace(), key, (String)value, 0, expire);
            isRetry = checkResultCode(rc, record);
            if (isRetry && doCheck(key, (String)value)) {
                isRetry = false;   // success actually
            }
        }
        if (count == MAX_RETRY_TIMES) {
            collector.collectDirtyRecord(record, "tair集群操作重试失败, key:"
                    + (String)key + "value: " + (String)value + "， rc: " + rc.getCode());
        }
        return 0;
    }

    private int retrySetCount(Serializable key, int count, int expire, Record record)
    {
        ResultCode rc = ResultCode.SERVERERROR;
        boolean isRetry = true;
        int retryCount = -1;
        while (isRetry && ++retryCount < MAX_RETRY_TIMES) {
            rc = tm.setCount(conf.getNamespace(), key, count, 0, expire);
            isRetry = checkResultCode(rc, record);
            if (isRetry && doCheck(key, ((Integer)count).toString())) {
                isRetry = false;   // success actually
            }
        }
        if (retryCount == MAX_RETRY_TIMES) {
            collector.collectDirtyRecord(record, "tair集群操作重试失败, key:"
                    + (String)key + "value: " + value + "， rc: " + rc.getCode());
        }
        return 0;
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
                String fieldString = record.getColumn(i).asString();
                if (null != fieldString){
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
}
