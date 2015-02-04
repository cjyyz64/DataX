package com.alibaba.datax.plugin.writer.otswriter;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSMode;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSSendBuffer;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.alibaba.datax.plugin.writer.otswriter.utils.ParseRecord;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;

public class OtsWriterSlaveProxy {
    
    private static final Logger LOG = LoggerFactory.getLogger(OtsWriterSlaveProxy.class);
    
    private OTSConf conf = null;
    private OTSSendBuffer buffer = null;
    private OTS ots = null;
    
    public void init(Configuration configuration) {
        
        LOG.info("Configuration : {}", Common.configurtionToNoSensitiveString(configuration));
        
        conf = GsonParser.jsonToConf(configuration.getString(OTSConst.OTS_CONF));
        
        ots = Common.getOTSInstance(conf);
    }
    
    public void close() {
        ots.shutdown();
    }
    
    public void write(RecordReceiver recordReceiver, TaskPluginCollector collector) throws Exception {
        LOG.info("write begin.");
        buffer = new OTSSendBuffer(ots, collector, conf);
        try {
            if (conf.getMode() == OTSMode.MULTI_VERSION) {
                writeForMultiVersion(recordReceiver, collector);
            } else {
                writeForNormal(recordReceiver, collector);
            }
        } finally {
            buffer.close();
        }
        LOG.info("write end.");
    }
    
    private void writeForMultiVersion(RecordReceiver recordReceiver, TaskPluginCollector collector) throws Exception {
        // Record format : {PK1, PK2, ...} {ColumnName} {TimeStamp} {Value}
        int expectColumnCount = conf.getPrimaryKeyColumn().size()+ 3;// 3表示{ColumnName} {TimeStamp} {Value}
        Record record = null;
        PrimaryKey oPk = null;
        List<Record> rowBuffer = new ArrayList<Record>();
        while ((record = recordReceiver.getFromReader()) != null) {
            
            LOG.debug("Record Raw: {}", record.toString());
            
            int columnCount = record.getColumnNumber();
            if (columnCount != expectColumnCount) {
                // 如果Column的个数和预期的个数不一致时，认为是系统故障或者用户配置Column错误，异常退出
                throw new IllegalArgumentException(
                        String.format(
                                OTSErrorMessage.RECORD_AND_COLUMN_SIZE_ERROR, 
                                columnCount, 
                                expectColumnCount
                            )
                        );
            }
            // check same row 
            PrimaryKey pk = null;
            try {
                pk = Common.getPKFromRecord(conf.getPrimaryKeyColumn(), record);
            } catch (IllegalArgumentException e) {
                collector.collectDirtyRecord(record, e.getMessage());
                continue;
            }
            if (oPk == null) {
                oPk = pk;
            }
            if ((oPk != null && !oPk.equals(pk)) || rowBuffer.size() == conf.getRestrictConf().getRowCellCountLimitation()) {
                OTSLine line = ParseRecord.parseMultiVersionRecordToOTSLine(
                        conf.getTableName(), 
                        conf.getOperation(), 
                        conf.getPrimaryKeyColumn(), 
                        conf.getAttributeColumn(),
                        rowBuffer,
                        collector
                );
                if (line != null) {
                    buffer.write(line);
                }
                rowBuffer.clear();
                oPk = pk;
            }
            rowBuffer.add(record);
        }
        
        // Flush剩余数据
        if (!rowBuffer.isEmpty()) {
            OTSLine line = ParseRecord.parseMultiVersionRecordToOTSLine(
                    conf.getTableName(), 
                    conf.getOperation(), 
                    conf.getPrimaryKeyColumn(), 
                    conf.getAttributeColumn(),
                    rowBuffer,
                    collector);
            if (line != null) {
                buffer.write(line);
            }
        }
    }
    
    private void writeForNormal(RecordReceiver recordReceiver, TaskPluginCollector collector) throws Exception {
        int expectColumnCount = conf.getPrimaryKeyColumn().size() + conf.getAttributeColumn().size();
        Record record = null;
        
        while ((record = recordReceiver.getFromReader()) != null) {
            int columnCount = record.getColumnNumber();
            if (columnCount != expectColumnCount) {
                // 如果Column的个数和预期的个数不一致时，认为是系统故障或者用户配置Column错误，异常退出
                throw new IllegalArgumentException(String.format(OTSErrorMessage.RECORD_AND_COLUMN_SIZE_ERROR, columnCount, expectColumnCount));
            }
            
            OTSLine line = ParseRecord.parseNormalRecordToOTSLine(
                        conf.getTableName(), 
                        conf.getOperation(), 
                        conf.getPrimaryKeyColumn(), 
                        conf.getAttributeColumn(), 
                        record,
                        conf.getTimestamp(),
                        collector);
            if (line != null) {
                buffer.write(line);
            }
        }
    }
}
