package com.alibaba.datax.plugin.writer.otswriter.utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.aliyun.openservices.ots.internal.ClientConfiguration;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.OTSClient;
import com.aliyun.openservices.ots.internal.model.ColumnValue;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.PrimaryKeySchema;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.internal.model.TableMeta;
import com.aliyun.openservices.ots.internal.utils.Pair;

public class Common {
    
    private static final Logger LOG = LoggerFactory.getLogger(Common.class);

    /**
     * 从record中分析出PK,如果分析成功，则返回PK,如果分析失败，则返回null，并记录数据到脏数据回收器中
     * @param pkColumns
     * @param r
     * @return
     * @throws OTSCriticalException 
     */
    public static PrimaryKey getPKFromRecord(Map<PrimaryKeySchema, Integer> pkColumns, Record r) throws OTSCriticalException {
        if (r.getColumnNumber() < pkColumns.size()) {
            throw new OTSCriticalException(String.format("Bug branch, the count(%d) of record < count(%d) of (pk) from config.", r.getColumnNumber(), pkColumns.size()));
        }
        try {
            List<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
            for (Entry<PrimaryKeySchema, Integer> en : pkColumns.entrySet()) {
                Column col = r.getColumn(en.getValue());
                PrimaryKeySchema expect = en.getKey();
                
                if (col.getRawData() == null) {
                    throw new IllegalArgumentException(String.format(OTSErrorMessage.PK_COLUMN_VALUE_IS_NULL_ERROR, expect.getName()));
                }

                PrimaryKeyValue pk = ColumnConversion.columnToPrimaryKeyValue(col, expect);
                pks.add(new PrimaryKeyColumn(expect.getName(), pk));
            }
            return new PrimaryKey(pks);
        } catch (IllegalArgumentException e) {
            LOG.warn("getPKFromRecord fail : {}", e.getMessage(), e);
            CollectorUtil.collect(r, e.getMessage());
            return null;
        }
    }

    /**
     * 从Record中解析ColumnValue，如果Record转换为ColumnValue失败，方法会返回null
     * @param pkCount
     * @param attrColumns
     * @param r
     * @return
     * @throws OTSCriticalException
     */
    public static List<Pair<String, ColumnValue>> getAttrFromRecord(int pkCount, List<OTSAttrColumn> attrColumns, Record r) throws OTSCriticalException {
        if (pkCount + attrColumns.size() != r.getColumnNumber()) {
            throw new OTSCriticalException(String.format("Bug branch, the count(%d) of record != count(%d) of (pk + column) from config.", r.getColumnNumber(), (pkCount + attrColumns.size())));
        }
        try {
            List<Pair<String, ColumnValue>> attr = new ArrayList<Pair<String, ColumnValue>>(r.getColumnNumber());
            for (int i = 0; i < attrColumns.size(); i++) {
                Column col = r.getColumn(i + pkCount);
                OTSAttrColumn expect = attrColumns.get(i);

                if (col.getRawData() == null) {
                    attr.add(new Pair<String, ColumnValue>(expect.getName(), null));
                    continue;
                }

                ColumnValue cv = ColumnConversion.columnToColumnValue(col, expect);
                attr.add(new Pair<String, ColumnValue>(expect.getName(), cv));
            }
            return attr;
        } catch (IllegalArgumentException e) {
            LOG.warn("getAttrFromRecord fail : {}", e.getMessage(), e);
            CollectorUtil.collect(r, e.getMessage());
            return null;
        }
    }

    public static long getDelaySendMillinSeconds(int hadRetryTimes, int initSleepInMilliSecond) {

        if (hadRetryTimes <= 0) {
            return 0;
        }

        int sleepTime = initSleepInMilliSecond;
        for (int i = 1; i < hadRetryTimes; i++) {
            sleepTime += sleepTime;
            if (sleepTime > 30000) {
                sleepTime = 30000;
                break;
            } 
        }
        return sleepTime;
    }
    
    public static OTS getOTSInstance(OTSConf conf) {
        ClientConfiguration clientConfigure = new ClientConfiguration();
        clientConfigure.setIoThreadCount(conf.getIoThreadCount());
        clientConfigure.setMaxConnections(conf.getConcurrencyWrite());
        clientConfigure.setSocketTimeoutInMillisecond(conf.getSocketTimeout());
        clientConfigure.setConnectionTimeoutInMillisecond(conf.getConnectTimeoutInMillisecond());
        clientConfigure.setRetryStrategy(new DefaultNoRetry());

        OTS ots = new OTSClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstanceName(),
                clientConfigure);
        return ots;
    }
    
    public static LinkedHashMap<PrimaryKeySchema,Integer> getPkColumnMapping(TableMeta meta, List<PrimaryKeySchema> attrColumns) throws OTSCriticalException {
        LinkedHashMap<PrimaryKeySchema,Integer> attrColumnMapping = new LinkedHashMap<PrimaryKeySchema,Integer>();
        for (Entry<String, PrimaryKeyType> en : meta.getPrimaryKeyMap().entrySet()) {
            // don't care performance
            int i = 0;
            for (; i < attrColumns.size(); i++) {
                if (attrColumns.get(i).getName().equals(en.getKey())) {
                    attrColumnMapping.put(attrColumns.get(i),  i);
                    break;
                }
            }
            if (i == attrColumns.size()) {
             // exception branch
                throw new OTSCriticalException(String.format(OTSErrorMessage.INPUT_PK_TYPE_NOT_MATCH_META_ERROR, en.getKey(), en.getValue())); 
            }
        }
        return attrColumnMapping;
    }
}
