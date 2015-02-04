package com.alibaba.datax.plugin.writer.otswriter.utils;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.Key;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSPKColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.Pair;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSBatchWriterRowTask.LineAndError;
import com.aliyun.openservices.ots.internal.ClientConfiguration;
import com.aliyun.openservices.ots.internal.ClientException;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.OTSClient;
import com.aliyun.openservices.ots.internal.OTSException;
import com.aliyun.openservices.ots.internal.model.ColumnValue;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;

public class Common {

    public static String getDetailMessage(Exception exception) {
        if (exception instanceof OTSException) {
            OTSException e = (OTSException) exception;
            return "OTSException[ErrorCode:" + e.getErrorCode() + ", ErrorMessage:" + e.getMessage() + ", RequestId:" + e.getRequestId() + "]";
        } else if (exception instanceof ClientException) {
            ClientException e = (ClientException) exception;
            return "ClientException[ErrorMessage:" + e.getMessage() + "]";
        } else if (exception instanceof IllegalArgumentException) {
            IllegalArgumentException e = (IllegalArgumentException) exception;
            return "IllegalArgumentException[ErrorMessage:" + e.getMessage() + "]";
        } else {
            return "Exception[ErrorMessage:" + exception.getMessage() + "]";
        }
    }

    /**
     * 从record中分析出PK
     * @param pkColumns
     * @param r
     * @return
     */
    public static PrimaryKey getPKFromRecord(List<OTSPKColumn> pkColumns, Record r) {
        List<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        int pkCount = pkColumns.size();
        for (int i = 0; i < pkCount; i++) {
            Column col = r.getColumn(i);
            OTSPKColumn expect = pkColumns.get(i);

            if (col.getRawData() == null) {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.PK_COLUMN_VALUE_IS_NULL_ERROR, expect.getName()));
            }

            PrimaryKeyValue pk = ColumnConversion.columnToPrimaryKeyValue(col, expect);
            pks.add(new PrimaryKeyColumn(expect.getName(), pk));
        }
        return new PrimaryKey(pks);
    }

    public static List<Pair<String, ColumnValue>> getAttrFromRecord(int pkCount, List<OTSAttrColumn> attrColumns, Record r) {
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
    
    public static void collectDirtyRecord(TaskPluginCollector collector, String errorMsg, List<Record> records) {
        for (Record r : records) {
            collector.collectDirtyRecord(r, errorMsg);
        }
    }

    public static void collectDirtyRecord(TaskPluginCollector collector, List<LineAndError> errors) {
        for (LineAndError re : errors) {
            collectDirtyRecord(collector,  re.getError().getMessage(), re.getLine().getRecords());
        }
    }

    public static void collectDirtyRecord(TaskPluginCollector collector, List<OTSLine> lines, String errorMsg) {
        for (OTSLine l : lines) {
            collectDirtyRecord(collector,  errorMsg, l.getRecords());
        }
    }
    
    // TODO fix
    public static String configurtionToNoSensitiveString(Configuration param) {
        Configuration outputParam = param.clone();
        outputParam.set(Key.OTS_ACCESSKEY, "*************");
        return outputParam.toJSON();
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
}
