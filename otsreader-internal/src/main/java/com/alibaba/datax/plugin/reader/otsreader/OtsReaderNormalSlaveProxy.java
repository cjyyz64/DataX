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

public class OtsReaderNormalSlaveProxy implements OtsReaderSlaveProxy {
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
        Record line = recordSender.createRecord();
        
        PrimaryKey pk = row.getPrimaryKey();
        for (OTSColumn column : conf.getColumn()) {
            if (column.getColumnType() == OTSColumn.OTSColumnType.NORMAL) {
                // 获取指定的列
                PrimaryKeyColumn value = pk.getPrimaryKeyColumn(column.getName());
                if (value != null) {
                    line.addColumn(TranformHelper.otsPrimaryKeyColumnToDataxColumn(value));
                } else {
                    Column c = row.getLatestColumn(column.getName());
                    if (c != null) {
                        throw new RuntimeException("Unimplement");
                    } else {
                        throw new RuntimeException("Unimplement");
                    }
                }
            } else {
                line.addColumn(column.getValue());
            }
            
        }
    }
    
    /**
     * 将获取到的数据根据用户配置Column的方式传递给datax
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
