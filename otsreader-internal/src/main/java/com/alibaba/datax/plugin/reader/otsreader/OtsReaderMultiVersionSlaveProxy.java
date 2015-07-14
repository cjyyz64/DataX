package com.alibaba.datax.plugin.reader.otsreader;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.Common;
import com.alibaba.datax.plugin.reader.otsreader.utils.OtsHelper;
import com.alibaba.datax.plugin.reader.otsreader.utils.TranformHelper;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.Column;
import com.aliyun.openservices.ots.internal.model.GetRangeResult;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.internal.model.Row;

public class OtsReaderMultiVersionSlaveProxy implements OtsReaderSlaveProxy {
    private OTSConf conf = null;
    private OTSRange range = null;
    private OTS ots = null;
    
    private static final Logger LOG = LoggerFactory.getLogger(OtsReaderMultiVersionSlaveProxy.class);
    
    @Override
    public void init(OTSConf conf, OTSRange range) {
        this.conf = conf;
        this.range = range;
        
        this.ots = OtsHelper.getOTSInstance(conf);
    }
    
    @Override
    public void close() {
        ots.shutdown();
    }
    
    private void sendToDatax(RecordSender recordSender, PrimaryKey pk, Column c) {
        Record line = recordSender.createRecord();
        //-------------------------
        // 四元组 pk,column name, timetamp, value
        //-------------------------
        
        // pk
        for( PrimaryKeyColumn pkc : pk.getPrimaryKeyColumns()) {
            line.addColumn(TranformHelper.otsPrimaryKeyColumnToDataxColumn(pkc));
        }
        // column name
        line.addColumn(new StringColumn(c.getName()));
        // Timestamp
        line.addColumn(new LongColumn(c.getTimestamp())); 
        // Value
        line.addColumn(TranformHelper.otsColumnToDataxColumn(c));
        
        recordSender.sendToWriter(line);
    }
    
    private void sendToDatax(RecordSender recordSender, Row row) {
        PrimaryKey pk = row.getPrimaryKey();
        List<OTSColumn> otsColumns = conf.getColumn();
        if (otsColumns.isEmpty()) {
            for (Column c : row.getColumns()){
                sendToDatax(recordSender, pk, c);
            }
        } else {
            for (Column c : row.getColumns()) {
                sendToDatax(recordSender, pk, c);
            }
        }
    }
    
    /**
     * 将获取到的数据采用4元组的方式传递给datax
     * @param recordSender
     * @param result
     */
    private void sendToDatax(RecordSender recordSender, GetRangeResult result) {
        LOG.debug("Per request get row count : " + result.getRows().size());
        for (Row row : result.getRows()) {
            sendToDatax(recordSender, row);
        }
    }
    
    @Override
    public void startRead(RecordSender recordSender) throws Exception {

        PrimaryKey inclusiveStartPrimaryKey = new PrimaryKey(range.getBegin());
        PrimaryKey exclusiveEndPrimaryKey = new PrimaryKey(range.getEnd());
        PrimaryKey next = inclusiveStartPrimaryKey;
        
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(conf.getTableName());
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setDirection(Common.getDirection(range.getBegin(), range.getEnd()));
        rangeRowQueryCriteria.setTimeRange(conf.getMulti().getTimeRange());
        rangeRowQueryCriteria.setMaxVersions(conf.getMulti().getMaxVersion());
        rangeRowQueryCriteria.addColumnsToGet(Common.toColumnToGet(conf.getColumn()));

        do{
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(next);
            GetRangeResult result = OtsHelper.getRange(
                    ots, 
                    rangeRowQueryCriteria, 
                    conf.getRetry(), 
                    conf.getRetryPauseInMillisecond());
            sendToDatax(recordSender, result);
            next = result.getNextStartPrimaryKey();
        } while(next != null);
    }
}
