package com.alibaba.datax.plugin.reader.otsreader;

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
import com.aliyun.openservices.ots.internal.model.TableMeta;

public class OtsReaderNormalSlaveProxy implements OtsReaderSlaveProxy {
    private OTSConf conf = null;
    private OTSRange range = null;
    private TableMeta meta = null;
    private OTS ots = null;
    
    @Override
    public void init(OTSConf conf, OTSRange range, TableMeta meta) {
        this.conf = conf;
        this.range = range;
        this.meta = meta;
        
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
                        line.addColumn(TranformHelper.otsColumnToDataxColumn(c));
                    } else {
                        // 这里使用StringColumn的无参构造函数构造对象，而不是用null，下
                        // 游（writer）应该通过获取Column，然后通过Column的数据接口的返回值
                        // 是否是null来判断改Column是否为null
                        // Datax其他插件的也是使用这种方式，约定俗成，并没有使用直接向record中注入null方式代表空
                        line.addColumn(new StringColumn());
                    }
                }
            } else {
                line.addColumn(column.getValue());
            }
        }
        recordSender.sendToWriter(line);
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
        rangeRowQueryCriteria.setDirection(Common.getDirection(range.getBegin(), range.getEnd()));
        rangeRowQueryCriteria.setMaxVersions(1);
        rangeRowQueryCriteria.addColumnsToGet(Common.toColumnToGet(conf.getColumn(), meta));

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
