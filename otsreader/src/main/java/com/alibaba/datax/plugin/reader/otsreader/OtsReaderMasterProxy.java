package com.alibaba.datax.plugin.reader.otsreader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConst;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.CommonUtils;
import com.alibaba.datax.plugin.reader.otsreader.utils.OtsModelUtils;
import com.alibaba.datax.plugin.reader.otsreader.utils.RangeSplitUtils;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.DescribeTableRequest;
import com.aliyun.openservices.ots.model.DescribeTableResult;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRangeResult;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.model.Row;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.TableMeta;

public class OtsReaderMasterProxy {
    
    private OTSConf conf = new OTSConf();
    
    private OTSRange range = new OTSRange();
    
    private OTSClient ots = null;
    
    private TableMeta meta = null;
    
    private Direction direction = null;
    
    public OTSConf getConf() {
        return conf;
    }
    
    private static final Logger LOG = LoggerFactory.getLogger(OtsReaderMasterProxy.class);
    
    /**
     * 1.检查参数是否为
     *     null，endpoint,accessid,accesskey,instance-name,table,column,range-begin,range-end,range-split
     * 2.检查参数是否为空字符串 
     *     endpoint,accessid,accesskey,instance-name,table
     * 3.检查是否为空数组
     *     column
     * 4.检查Range的类型个个数是否和PrimaryKey匹配
     *     column,range-begin,range-end
     * 5.检查Range Split 顺序和类型是否Range一致，类型是否于PartitionKey一致
     *     column-split
     * @param param
     * @throws Exception
     */
    public void init(Configuration param) throws Exception {        
        // 必选参数
        conf.setEndpoint(CommonUtils.checkStringParamAndGet(param, Key.OTS_ENDPOINT)); 
        conf.setAccessId(CommonUtils.checkStringParamAndGet(param, Key.OTS_ACCESSID)); 
        conf.setAccesskey(CommonUtils.checkStringParamAndGet(param, Key.OTS_ACCESSKEY)); 
        conf.setInstanceName(CommonUtils.checkStringParamAndGet(param, Key.OTS_INSTANCE_NAME)); 
        conf.setTableName(CommonUtils.checkStringParamAndGet(param, Key.TABLE_NAME)); 
        
        conf.setColumns(OtsModelUtils.parseOTSColumnList(CommonUtils.checkListParamAndGet(param, Key.COLUMN, true)));
        conf.setRangeBegin(OtsModelUtils.parsePrimaryKey(CommonUtils.checkListParamAndGet(param, Key.RANGE_BEGIN, true)));
        conf.setRangeEnd(OtsModelUtils.parsePrimaryKey(CommonUtils.checkListParamAndGet(param, Key.RANGE_END, true)));

        // 默认参数
        conf.setRetry(param.getInt(Key.RETRY, 20));
        conf.setSleepInMilliSecond(param.getInt(Key.SLEEP_IN_MILLI_SECOND, 50));
        
        ots = new OTSClient(
                this.conf.getEndpoint(),
                this.conf.getAccessId(),
                this.conf.getAccesskey(),
                this.conf.getInstanceName());
        
        meta = getTableMeta(ots, conf.getTableName());
        
        range.setBegin(CommonUtils.checkInputPrimaryKeyAndGet(meta, this.conf.getRangeBegin()));
        range.setEnd(CommonUtils.checkInputPrimaryKeyAndGet(meta, this.conf.getRangeEnd()));
        
        int cmp = CommonUtils.compareRangeBeginAndEnd(meta, range.getBegin(), range.getEnd()) ;
        
        if (cmp > 0) {
            direction = Direction.BACKWARD;
        } else if (cmp < 0) {
            direction = Direction.FORWARD;
        } else {
            throw new IllegalArgumentException("Value of 'range-begin' equal value of 'range-end'.");
        }
        
        List<Object> defaultPoints = new ArrayList<Object>();
        List<PrimaryKeyValue> points = OtsModelUtils.parsePrimaryKey(param.getList(Key.RANGE_SPLIT, defaultPoints));
        CommonUtils.checkInputRangeSplit(meta, direction, points);
        conf.setRangeSplit(points);
       
        LOG.debug(param.toJSON());
    }
    
    public List<Configuration> split(int num) throws Exception {
        List<Configuration> configurations = new ArrayList<Configuration>();
        
        TableMeta meta = getTableMeta(ots, this.conf.getTableName());
        
        List<OTSRange> ranges = null;
        
        if (!this.conf.getRangeSplit().isEmpty()) { // 用户显示指定了拆分范围
            LOG.info("Begin specifySplitRange");
            ranges = specifySplitRange(meta, range, this.conf.getRangeSplit());
            LOG.info("End specifySplitRange");
        } else { // 采用默认的切分算法 
            LOG.info("Begin defaultSplitRange");
            ranges = defaultSplitRange(ots, meta, range, num);
            LOG.info("End defaultSplitRange");
        }
        
        for (OTSRange item : ranges) {
            Configuration configuration = Configuration.newDefault();
            configuration.set(OTSConst.OTS_CONF, CommonUtils.confToString(this.conf));
            configuration.set(OTSConst.OTS_RANGE, CommonUtils.rangeToString(item));
            configuration.set(OTSConst.OTS_DIRECTION, CommonUtils.directionToString(direction));
            configurations.add(configuration);
        }
        
        return configurations;
    }
    
    // private function
    
    private TableMeta getTableMeta(OTSClient ots, String tableName) throws Exception {
        int remainingRetryTimes = this.conf.getRetry();
        while (true) {
            try {
                DescribeTableRequest describeTableRequest = new DescribeTableRequest();
                describeTableRequest.setTableName(tableName);
                DescribeTableResult result = ots.describeTable(describeTableRequest);
                TableMeta tableMeta = result.getTableMeta();
                return tableMeta;
            } catch (Exception e) {
                remainingRetryTimes = CommonUtils.getRetryTimes(e, remainingRetryTimes);
                if (remainingRetryTimes > 0) {
                    try {
                        Thread.sleep(this.conf.getSleepInMilliSecond());
                    } catch (InterruptedException ee) {
                        LOG.warn(ee.getMessage());
                    }
                } else {
                    LOG.error("Retry times more than limition", e);
                    throw e;
                }
            }
        }
    }
    
    private RowPrimaryKey getPKOfFirstRow(
            OTSRange range , Direction direction) throws Exception {

        RangeRowQueryCriteria cur = new RangeRowQueryCriteria(this.conf.getTableName());
        cur.setInclusiveStartPrimaryKey(range.getBegin());
        cur.setExclusiveEndPrimaryKey(range.getEnd());
        cur.setLimit(1);
        cur.setColumnsToGet(CommonUtils.getPrimaryKeyNameList(meta));
        cur.setDirection(direction);

        int remainingRetryTimes = this.conf.getRetry();
        while (true) {
            try {
                RowPrimaryKey ret = new RowPrimaryKey();
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(cur);
                GetRangeResult result = ots.getRange(request);
                List<Row> rows = result.getRows();
                if(rows.isEmpty()) {
                    return null;// no data
                } 
                Row row = rows.get(0);
             
                Map<String, PrimaryKeyType> pk = meta.getPrimaryKey();
                for (String key:pk.keySet()) {
                    ColumnValue v = row.getColumns().get(key);
                    if (v.getType() ==  ColumnType.INTEGER) {
                        ret.addPrimaryKeyColumn(key, PrimaryKeyValue.fromLong(v.asLong()));
                    } else {
                        ret.addPrimaryKeyColumn(key, PrimaryKeyValue.fromString(v.asString()));
                    }
                }
                return ret;
            } catch (Exception e) {
                remainingRetryTimes = CommonUtils.getRetryTimes(e, remainingRetryTimes);
                if (remainingRetryTimes > 0) {
                    try {
                        Thread.sleep(this.conf.getSleepInMilliSecond());
                    } catch (InterruptedException ee) {
                        LOG.warn(ee.getMessage());
                    }
                } else {
                    LOG.error("Retry times more than limition", e);
                    throw e;
                }
            }
        }
    }
    
    private List<OTSRange> defaultSplitRange(OTSClient ots, TableMeta meta, OTSRange range, int num) throws Exception {
        OTSRange reverseRange = new OTSRange();
        reverseRange.setBegin(range.getEnd());
        reverseRange.setEnd(range.getBegin());
        
        Direction reverseDirection = (direction == Direction.FORWARD ? Direction.BACKWARD : Direction.FORWARD);
        
        RowPrimaryKey realBegin = getPKOfFirstRow(range, direction);
        RowPrimaryKey realEnd   = getPKOfFirstRow(reverseRange, reverseDirection);
        
        if (realBegin == null || realEnd == null) {
            // 因为如果其中一行为空，表示这个范围内至多有一行数据
            // 所以不再细分，直接使用用户定义的范围
            List<OTSRange> ranges = new ArrayList<OTSRange>();
            ranges.add(range);
            return ranges;
        }
        
        List<OTSRange> ranges = RangeSplitUtils.splitRange(meta, realBegin, realEnd, num);
        
        // replace first and last
        OTSRange first = ranges.get(0);
        OTSRange last = ranges.get(ranges.size() - 1);
        
        first.setBegin(range.getBegin());
        last.setEnd(range.getEnd());
        
        return ranges;
    }
    
    private List<OTSRange> specifySplitRange(TableMeta meta, OTSRange range, List<PrimaryKeyValue> points) {
        return RangeSplitUtils.specifySplitRange(meta, conf, range, points);
    }
}
