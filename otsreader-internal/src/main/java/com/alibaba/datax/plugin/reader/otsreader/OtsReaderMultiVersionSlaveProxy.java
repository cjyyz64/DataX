package com.alibaba.datax.plugin.reader.otsreader;

import java.util.List;

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
    
    private void sendToDatax(RecordSender recordSender, Row row) {
        PrimaryKey pk = row.getPrimaryKey();
        for (OTSColumn column : conf.getColumn()) {
            // 获取指定的列
            List<Column> columns = row.getColumn(column.getName());
            for (Column c : columns) {
                Record line = recordSender.createRecord();
                //-------------------------
                // 四元组 pk,column name, timetamp, value
                //-------------------------
                
                // pk
                for( PrimaryKeyColumn pkc : pk.getPrimaryKeyColumns()) {
                    line.addColumn(TranformHelper.otsPrimaryKeyColumnToDataxColumn(pkc));
                }
                // column name
                line.addColumn(new StringColumn(column.getName()));
                // Timestamp
                line.addColumn(new LongColumn(c.getTimestamp())); 
                // Value
                line.addColumn(TranformHelper.otsColumnToDataxColumn(c));
                
                recordSender.sendToWriter(line);
            }
        }
    }
    
    /**
     * 将获取到的数据采用4元组的方式传递给datax
     * @param recordSender
     * @param result
     */
    private void sendToDatax(RecordSender recordSender, GetRangeResult result) {
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
        rangeRowQueryCriteria.setTimeRange(conf.getMulti().getTimeRange());
        rangeRowQueryCriteria.setMaxVersions(conf.getMulti().getMaxVersion());
        rangeRowQueryCriteria.addColumnsToGet(Common.toColumnToGet(conf.getColumn()));

        do{
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(next);
            GetRangeResult result = OtsHelper.getRange(
                    ots, 
                    rangeRowQueryCriteria, 
                    conf.getRetry(), 
                    conf.getSleepInMilliSecond());
            sendToDatax(recordSender, result);
            next = result.getNextStartPrimaryKey();
        } while(next != null);
    }
}
