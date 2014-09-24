package com.alibaba.datax.plugin.reader.otsreader;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConst;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.CommonUtils;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRangeResult;
import com.aliyun.openservices.ots.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.model.Row;
import com.aliyun.openservices.ots.model.RowPrimaryKey;

public class OtsReaderSlaveProxy {
    private static final Logger LOG = LoggerFactory.getLogger(OtsReaderSlaveProxy.class);

    public void read(
            RecordSender sender, 
            Configuration configuration) throws Exception {
        LOG.info("====================== Configuration =======================");
        LOG.info(configuration.getString(OTSConst.OTS_CONF));
        LOG.info(configuration.getString(OTSConst.OTS_RANGE));
        LOG.info(configuration.getString(OTSConst.OTS_DIRECTION));
        LOG.info("============================================================");
        OTSConf conf = CommonUtils.stringToConf(configuration.getString(OTSConst.OTS_CONF));
        OTSRange range = CommonUtils.stringToRange(configuration.getString(OTSConst.OTS_RANGE));
        Direction direction = CommonUtils.stringToDirection(configuration.getString(OTSConst.OTS_DIRECTION));;
        
        OTSClient ots = new OTSClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccesskey(),
                conf.getInstanceName());

        RowPrimaryKey token = range.getBegin();
        List<String> columns = CommonUtils.getNormalColumnNameList(conf.getColumns());
        do {
            int remainingRetryTimes = conf.getRetry();
            while (true) {
                try {
                    RangeRowQueryCriteria cur = new RangeRowQueryCriteria(conf.getTableName());
                    cur.setDirection(direction);
                    cur.setColumnsToGet(columns);
                    cur.setLimit(-1);
                    cur.setInclusiveStartPrimaryKey(token);
                    cur.setExclusiveEndPrimaryKey(range.getEnd());
                    
                    GetRangeRequest request = new GetRangeRequest();
                    request.setRangeRowQueryCriteria(cur);

                    GetRangeResult result = ots.getRange(request);
                    token = result.getNextStartPrimaryKey();
                    for (Row row : result.getRows()) {
                        Record line = sender.createRecord();
                        line = CommonUtils.parseRowToLine(row, conf.getColumns(), line); 
                        sender.sendToWriter(line);
                    }
                    break;
                } catch (Exception e) {
                    remainingRetryTimes = CommonUtils.getRetryTimes(e, remainingRetryTimes);
                    if (remainingRetryTimes > 0) {
                        try {
                            Thread.sleep(conf.getSleepInMilliSecond());
                        } catch (InterruptedException ee) {
                            LOG.warn(ee.getMessage());
                        }
                    } else {
                        LOG.error("Retry times more than limition", e);
                        throw e;
                    }
                }
            }
        } while (token != null);
    }
}
