package com.alibaba.datax.plugin.reader.otsreader.common;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSCriticalException;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSMode;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.Common;
import com.alibaba.datax.plugin.reader.otsreader.utils.CompareHelper;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.Column;
import com.aliyun.openservices.ots.internal.model.GetRangeRequest;
import com.aliyun.openservices.ots.internal.model.GetRangeResult;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.internal.model.Row;
import com.aliyun.openservices.ots.internal.model.TableMeta;
import com.google.gson.Gson;

import static org.junit.Assert.*;

public class AssertHelper {
    
    private static final Logger LOG = LoggerFactory.getLogger(AssertHelper.class);
    
    public static int compareBytes(byte[] b1, byte[] b2) {
        int size = b1.length < b2.length ? b1.length : b2.length;
        for (int i = 0; i < size; i++) {
            int r = b1[i] - b2[i];
            if (r != 0) {
                return r;
            }
        }
        if (b1.length > b2.length) {
            return 1;
        } else if (b1.length < b2.length){
            return -1;
        } else {
            return 0;
        }
    }
    
    public static String getRecordString(Record r) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < r.getColumnNumber(); i++) {
            com.alibaba.datax.common.element.Column c = r.getColumn(i);
            if (c == null) {
                sb.append("N/A, ");
            } else {
                switch (c.getType()) {
                case STRING:  sb.append(c.asString() + "(STRING), "); break;
                case LONG:  sb.append(c.asLong() + "(LONG), "); break;
                case DOUBLE:  sb.append(c.asDouble() + "(DOUBLE), "); break;
                case BOOL:  sb.append(c.asBoolean() + "(BOOL), "); break;
                case BYTES: sb.append("(BYTES), "); ; break;
                default:
                    throw new RuntimeException("Unsupport the type.");
                }
            }
            
        }
        return sb.toString();
    }

    public static void assertOTSRange(OTSRange src, OTSRange target) {
        if ((src == null && target != null) || (src != null && target == null)) {
            fail();
        }
        if (src != null && target != null) {
            if ((src.getBegin() == null && target.getBegin() != null) || (src.getBegin() != null && target.getBegin() == null)) {
                fail();
            }
            if ((src.getEnd() == null && target.getEnd() != null) || (src.getEnd() != null && target.getEnd() == null)) {
                fail();
            }
            if ((src.getSplit() == null && target.getSplit() != null) || (src.getSplit() != null && target.getSplit() == null)) {
                fail();
            }
            if (((src.getBegin() != null && target.getBegin() != null) && CompareHelper.comparePrimaryKeyColumnList(src.getBegin(), target.getBegin()) != 0) ) {
                fail();
            }
            if (((src.getEnd() != null && target.getEnd() != null) && CompareHelper.comparePrimaryKeyColumnList(src.getEnd(), target.getEnd()) != 0) ) {
                fail();
            }
            if (((src.getSplit() != null && target.getSplit() != null) && CompareHelper.comparePrimaryKeyColumnList(src.getSplit(), target.getSplit()) != 0) ) {
                fail();
            }
        }
    }
    
    public static void assertOTSColumn(List<OTSColumn> src, List<OTSColumn> target) {
        if ((src == null && target != null) || (src != null && target == null)) {
            fail();
        }
        Gson g = new Gson();
        if (src != null && target != null) {
            assertEquals(src.size(), target.size());
            for (int i = 0; i < src.size(); i++) {
                OTSColumn s = src.get(i);
                OTSColumn t = target.get(i);
                
                assertEquals(s.getName(), t.getName());
                assertEquals(s.getColumnType(), t.getColumnType());
                
                assertEquals(g.toJson(s.getValue()), g.toJson(t.getValue()));
            }
        }
    }
    
    private static void checkRecord(Record expect, Record src) {
        LOG.debug("\nRecord1:{} \nRecord2:{}", getRecordString(expect), getRecordString(src));
        if (expect.getColumnNumber() != src.getColumnNumber()) {
            LOG.error("Size({}) of Record 1  not equal size({}) of Record 2", expect.getColumnNumber(), src.getColumnNumber());
            fail();
        }
        for (int j = 0; j < expect.getColumnNumber(); j++) {
            com.alibaba.datax.common.element.Column c1 = expect.getColumn(j);
            com.alibaba.datax.common.element.Column c2 = src.getColumn(j);
            if (c1 == null || c2 == null) {
                if (c1 != c2) {
                    fail();
                }
                continue;
            }
            
            if (c1.getType() != c2.getType()) {
                LOG.error("Type 1 not match type 2, {}, {}", c1.getType(), c2.getType());
                fail();
            }
            if (c1.getRawData() == null && c2.getRawData() == null) {
                continue;
            } else if (c1.getRawData() == null && c2.getRawData() != null) {
                LOG.error("column1 is null but column2 is not null.");
                fail();
            } else if (c1.getRawData() != null && c2.getRawData() == null) {
                LOG.error("column2 is null but column1 is not null.");
                fail();
            } else {
                switch (c1.getType()) {
                    case STRING: 
                        if (!(c1.asString().equals(c2.asString()))) {
                            LOG.error("not equal. {} != {}", c1.asString(), c2.asString());
                            fail();
                        }
                        break;
                    case LONG:
                        if (!(c1.asLong().longValue() == c2.asLong().longValue())) {
                            LOG.error("not equal. {} != {}", c1.asLong(), c2.asLong());
                            fail();
                        }
                        break;
                    case DOUBLE:
                        if (!(c1.asDouble().doubleValue() == c2.asDouble().doubleValue())) {
                            LOG.error("not equal. {} != {}", c1.asDouble(), c2.asDouble());
                            fail();
                        }
                        break;
                    case BOOL:
                        if (!(c1.asBoolean().booleanValue() == c2.asBoolean().booleanValue())) {
                            LOG.error("not equal. {} != {}", c1.asBoolean(), c2.asBoolean());
                            fail();
                        }
                        break;
                    case BYTES:  
                        if (!(compareBytes(c1.asBytes(), c2.asBytes()) == 0)) {
                            LOG.error("not equal bytes.");
                            fail();
                        }
                        break;
                    default:
                        throw new RuntimeException("Unsupport the type.");
                    }
            }
            
        }
    }
    
    private static void checkRowAndRecordForNormal(List<OTSColumn> columns, Row row, Record src) {
        Record expect = ExchangeHelper.parseRowToRecord(columns, row);
        checkRecord(expect, src);
    }
    
    private static void assertRecordsForNormal(OTS ots, TableMeta meta, OTSConf conf, List<Record> records) throws OTSCriticalException {
        OTSRange range = conf.getRange();
        
        PrimaryKey inclusiveStartPrimaryKey = new PrimaryKey(range.getBegin());
        PrimaryKey exclusiveEndPrimaryKey = new PrimaryKey(range.getEnd());
        PrimaryKey next = inclusiveStartPrimaryKey;
        
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(conf.getTableName());
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setDirection(Common.getDirection(range.getBegin(), range.getEnd()));
        rangeRowQueryCriteria.setMaxVersions(1);
        rangeRowQueryCriteria.addColumnsToGet(Common.toColumnToGet(conf.getColumn(), meta));
        
        List<Row> rows = new ArrayList<Row>();
        do{
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(next);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(rangeRowQueryCriteria);
            GetRangeResult result = ots.getRange(request);
            rows.addAll(result.getRows());
            next = result.getNextStartPrimaryKey();
        } while (next != null);
        
        // check
        if (rows.size() != records.size()) {
            fail("Expect rows size:"+ rows.size() +", but rows size:" + records.size());
        }
        
        int size = rows.size();
        for (int i = 0; i < size; i++) {
            checkRowAndRecordForNormal(conf.getColumn(), rows.get(i), records.get(i));
        }
    }
    
    private static void checkRowsAndRecordsForMulti(List<OTSColumn> columns, List<Row> rows, List<Record> records) {
        int cellCount = 0;
        for (Row row : rows) {
            cellCount += row.getColumns().length;
        }
        
        if (cellCount != records.size()) {
            fail("Expect cell size:"+ rows.size() +", but record size:" + records.size());
        }
        
        int index = 0;
        
        for (Row row : rows) {
            for (Column c : row.getColumns()) {
                Record r1 = ExchangeHelper.parseColumnToRecord(row.getPrimaryKey(), c);
                Record r2 = records.get(index++);
                checkRecord(r1, r2);
            }
        }
    }
    
    private static void assertRecordsForMulti(OTS ots, TableMeta meta, OTSConf conf, List<Record> records) throws OTSCriticalException {
        OTSRange range = conf.getRange();
        
        PrimaryKey inclusiveStartPrimaryKey = new PrimaryKey(range.getBegin());
        PrimaryKey exclusiveEndPrimaryKey = new PrimaryKey(range.getEnd());
        PrimaryKey next = inclusiveStartPrimaryKey;
        
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(conf.getTableName());
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setDirection(Common.getDirection(range.getBegin(), range.getEnd()));
        rangeRowQueryCriteria.setTimeRange(conf.getMulti().getTimeRange());
        rangeRowQueryCriteria.setMaxVersions(conf.getMulti().getMaxVersion());
        rangeRowQueryCriteria.addColumnsToGet(Common.toColumnToGet(conf.getColumn(), meta));
        
        List<Row> rows = new ArrayList<Row>();
        do{
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(next);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(rangeRowQueryCriteria);
            GetRangeResult result = ots.getRange(request);
            rows.addAll(result.getRows());
            next = result.getNextStartPrimaryKey();
        } while (next != null);
        
        // TODO check
        checkRowsAndRecordsForMulti(conf.getColumn(), rows, records);
    }
    
    public static void assertRecords(OTS ots, TableMeta meta, OTSConf conf, List<Record> noteRecordForTest) throws OTSCriticalException {
        if (conf.getMode() == OTSMode.MULTI_VERSION) {
            assertRecordsForMulti(ots, meta, conf, noteRecordForTest);
        } else {
            assertRecordsForNormal(ots, meta, conf, noteRecordForTest);
        }
    }
}
