package com.alibaba.datax.plugin.reader.otsreader.unittest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.CommonUtils;
import com.alibaba.datax.plugin.reader.otsreader.utils.RangeSplitUtils;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.TableMeta;

/**
 * 测试功能点
 * http://wiki.aliyun-inc.com/projects/apsara/wiki/datax_ots_reader_testing
 * @author wanhong.chenwh@alibaba-inc.com
 *
 */
public class RangeSplitUtilsSpecifyUnittest {

    // =========================================================================
    // 1.基础测试，测试切分功能正常，切分的范围符合预期
    // =========================================================================
    @Test
    public void testBase1PK() {
        TableMeta meta = new TableMeta("test");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        
        OTSConf conf = new OTSConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromLong(10000));
        
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromLong(-21312));
        
        conf.setRangeBegin(rangeBegin);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeBegin()));
        range.setEnd(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeEnd()));
        
        {
            List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();
            splits.add(PrimaryKeyValue.fromLong(99999));
            splits.add(PrimaryKeyValue.fromLong(5000));
            splits.add(PrimaryKeyValue.fromLong(2000));
            splits.add(PrimaryKeyValue.fromLong(-1211));
            
            List<OTSRange> ranges = RangeSplitUtils.specifySplitRange(meta, conf, range, splits);
            
            assertEquals(4, ranges.size());
            
            RowPrimaryKey r0 = new RowPrimaryKey();
            r0.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(10000));

            
            RowPrimaryKey r1 = new RowPrimaryKey();
            r1.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(5000));

            
            RowPrimaryKey r2 = new RowPrimaryKey();
            r2.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(2000));

            
            RowPrimaryKey r3 = new RowPrimaryKey();
            r3.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(-1211));

            
            RowPrimaryKey r4 = new RowPrimaryKey();
            r4.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(-21312));

            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r0, ranges.get(0).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(0).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(1).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(1).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(2).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r3, ranges.get(2).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r3, ranges.get(3).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r4, ranges.get(3).getEnd()) == 0);
        }
    }
    
    @Test
    public void testBase2PK() {
        TableMeta meta = new TableMeta("test");
        meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
        meta.addPrimaryKeyColumn("pk4", PrimaryKeyType.INTEGER);
        
        OTSConf conf = new OTSConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromString("杭州"));
        rangeBegin.add(PrimaryKeyValue.fromLong(-19890));
        
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromString("北京"));
        rangeEnd.add(PrimaryKeyValue.fromLong(11887));
        
        conf.setRangeBegin(rangeBegin);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeBegin()));
        range.setEnd(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeEnd()));
        
        {
            List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();
            splits.add(PrimaryKeyValue.fromString("0"));
            splits.add(PrimaryKeyValue.fromString("1"));
            splits.add(PrimaryKeyValue.fromString("2"));
            splits.add(PrimaryKeyValue.fromString("3"));
            
            List<OTSRange> ranges = RangeSplitUtils.specifySplitRange(meta, conf, range, splits);
            
            assertEquals(1, ranges.size());
            
            RowPrimaryKey r0 = new RowPrimaryKey();
            r0.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("杭州"));
            r0.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(-19890));
            
            RowPrimaryKey r1 = new RowPrimaryKey();
            r1.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("北京"));
            r1.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(11887));
            
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r0, ranges.get(0).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(0).getEnd()) == 0);
        }
    }
    
    @Test
    public void testBase4PK() {
        TableMeta meta = new TableMeta("test");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
        meta.addPrimaryKeyColumn("pk4", PrimaryKeyType.INTEGER);
        
        OTSConf conf = new OTSConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromString("0"));
        rangeBegin.add(PrimaryKeyValue.fromLong(1001));
        rangeBegin.add(PrimaryKeyValue.fromString("杭州"));
        rangeBegin.add(PrimaryKeyValue.fromLong(-19890));
        
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromString("A"));
        rangeEnd.add(PrimaryKeyValue.fromLong(-21312));
        rangeEnd.add(PrimaryKeyValue.fromString("北京"));
        rangeEnd.add(PrimaryKeyValue.fromLong(11887));
        
        conf.setRangeBegin(rangeBegin);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeBegin()));
        range.setEnd(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeEnd()));
        
        {
            List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();
            splits.add(PrimaryKeyValue.fromString("0"));
            splits.add(PrimaryKeyValue.fromString("1"));
            splits.add(PrimaryKeyValue.fromString("2"));
            splits.add(PrimaryKeyValue.fromString("3"));
            
            List<OTSRange> ranges = RangeSplitUtils.specifySplitRange(meta, conf, range, splits);
            
            assertEquals(4, ranges.size());
            
            RowPrimaryKey r0 = new RowPrimaryKey();
            r0.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("0"));
            r0.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(1001));
            r0.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("杭州"));
            r0.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(-19890));
            
            RowPrimaryKey r1 = new RowPrimaryKey();
            r1.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("1"));
            r1.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r2 = new RowPrimaryKey();
            r2.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("2"));
            r2.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r3 = new RowPrimaryKey();
            r3.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("3"));
            r3.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r3.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r3.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r4 = new RowPrimaryKey();
            r4.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("A"));
            r4.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(-21312));
            r4.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("北京"));
            r4.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(11887));
            
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r0, ranges.get(0).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(0).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(1).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(1).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(2).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r3, ranges.get(2).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r3, ranges.get(3).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r4, ranges.get(3).getEnd()) == 0);
        }
    }

    // =========================================================================
    // 2.简单的功能测试，测用户指定的范围，（6种情况）
    // =========================================================================
    @Test
    public void testS1() {
        TableMeta meta = new TableMeta("test");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
        meta.addPrimaryKeyColumn("pk4", PrimaryKeyType.INTEGER);
        
        OTSConf conf = new OTSConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromString("AAAA"));
        rangeBegin.add(PrimaryKeyValue.fromLong(1001));
        rangeBegin.add(PrimaryKeyValue.fromString("!!!!"));
        rangeBegin.add(PrimaryKeyValue.fromLong(-19890));
        
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromString("mmmm"));
        rangeEnd.add(PrimaryKeyValue.fromLong(-21312));
        rangeEnd.add(PrimaryKeyValue.fromString("$$$$"));
        rangeEnd.add(PrimaryKeyValue.fromLong(11887));
        
        conf.setRangeBegin(rangeBegin);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeBegin()));
        range.setEnd(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeEnd()));
        
        // 没有交集
        {
            List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();
            splits.add(PrimaryKeyValue.fromString("0"));
            splits.add(PrimaryKeyValue.fromString("1"));
            splits.add(PrimaryKeyValue.fromString("2"));
            splits.add(PrimaryKeyValue.fromString("3"));
            
            List<OTSRange> ranges = RangeSplitUtils.specifySplitRange(meta, conf, range, splits);
            
            assertEquals(1, ranges.size());
            
            RowPrimaryKey r0 = new RowPrimaryKey();
            r0.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("AAAA"));
            r0.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(1001));
            r0.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("!!!!"));
            r0.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(-19890));
            
            RowPrimaryKey r1 = new RowPrimaryKey();
            r1.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("mmmm"));
            r1.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(-21312));
            r1.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("$$$$"));
            r1.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(11887));
            
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r0, ranges.get(0).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(0).getEnd()) == 0);
        }
        // 部分交集
        {
            List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();
            splits.add(PrimaryKeyValue.fromString("0"));
            splits.add(PrimaryKeyValue.fromString("AAAA"));
            splits.add(PrimaryKeyValue.fromString("BBBB"));
            splits.add(PrimaryKeyValue.fromString("CCCC"));
            
            List<OTSRange> ranges = RangeSplitUtils.specifySplitRange(meta, conf, range, splits);
            
            assertEquals(3, ranges.size());
            
            RowPrimaryKey r0 = new RowPrimaryKey();
            r0.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("AAAA"));
            r0.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(1001));
            r0.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("!!!!"));
            r0.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(-19890));
            
            RowPrimaryKey r1 = new RowPrimaryKey();
            r1.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("BBBB"));
            r1.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r2 = new RowPrimaryKey();
            r2.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("CCCC"));
            r2.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r3 = new RowPrimaryKey();
            r3.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("mmmm"));
            r3.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(-21312));
            r3.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("$$$$"));
            r3.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(11887));
            
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r0, ranges.get(0).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(0).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(1).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(1).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(2).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r3, ranges.get(2).getEnd()) == 0);
        }
        // 部分交集
        {
            List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();

            splits.add(PrimaryKeyValue.fromString("BBBB"));
            splits.add(PrimaryKeyValue.fromString("CCCC"));
            splits.add(PrimaryKeyValue.fromString("ZZZZ"));
            splits.add(PrimaryKeyValue.fromString("zzzz"));
            
            List<OTSRange> ranges = RangeSplitUtils.specifySplitRange(meta, conf, range, splits);
            
            assertEquals(4, ranges.size());
            
            RowPrimaryKey r0 = new RowPrimaryKey();
            r0.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("AAAA"));
            r0.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(1001));
            r0.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("!!!!"));
            r0.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(-19890));
            
            RowPrimaryKey r1 = new RowPrimaryKey();
            r1.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("BBBB"));
            r1.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r2 = new RowPrimaryKey();
            r2.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("CCCC"));
            r2.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r3 = new RowPrimaryKey();
            r3.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("ZZZZ"));
            r3.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r3.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r3.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r4 = new RowPrimaryKey();
            r4.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("mmmm"));
            r4.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(-21312));
            r4.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("$$$$"));
            r4.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(11887));
            
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r0, ranges.get(0).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(0).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(1).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(1).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(2).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r3, ranges.get(2).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r3, ranges.get(3).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r4, ranges.get(3).getEnd()) == 0);
        }
        
        // 包含
        {
            List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();

            splits.add(PrimaryKeyValue.fromString("CCCC"));
            splits.add(PrimaryKeyValue.fromString("ZZZZ"));
            
            List<OTSRange> ranges = RangeSplitUtils.specifySplitRange(meta, conf, range, splits);
            
            assertEquals(3, ranges.size());
            
            RowPrimaryKey r0 = new RowPrimaryKey();
            r0.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("AAAA"));
            r0.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(1001));
            r0.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("!!!!"));
            r0.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(-19890));
            
            RowPrimaryKey r1 = new RowPrimaryKey();
            r1.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("CCCC"));
            r1.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r2 = new RowPrimaryKey();
            r2.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("ZZZZ"));
            r2.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r3 = new RowPrimaryKey();
            r3.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("mmmm"));
            r3.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(-21312));
            r3.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("$$$$"));
            r3.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(11887));
            
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r0, ranges.get(0).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(0).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(1).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(1).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(2).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r3, ranges.get(2).getEnd()) == 0);
        }
    }
    // =========================================================================
    // 3.验证特殊字符串的切分
    // =========================================================================
    @Test
    public void testSpcialChar() {
        TableMeta meta = new TableMeta("test");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
        meta.addPrimaryKeyColumn("pk4", PrimaryKeyType.INTEGER);
        
        OTSConf conf = new OTSConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.fromLong(1001));
        rangeBegin.add(PrimaryKeyValue.fromString("!!!!"));
        rangeBegin.add(PrimaryKeyValue.fromLong(-19890));
        
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.fromLong(-21312));
        rangeEnd.add(PrimaryKeyValue.fromString("$$$$"));
        rangeEnd.add(PrimaryKeyValue.fromLong(11887));
        
        conf.setRangeBegin(rangeBegin);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeBegin()));
        range.setEnd(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeEnd()));
        
        {
            List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();

            splits.add(PrimaryKeyValue.fromString("数据验证符合预期"));
            splits.add(PrimaryKeyValue.fromString("!!!!!!!!!!!!!"));
            
            List<OTSRange> ranges = RangeSplitUtils.specifySplitRange(meta, conf, range, splits);
            
            assertEquals(3, ranges.size());
            
            RowPrimaryKey r0 = new RowPrimaryKey();
            r0.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MAX);
            r0.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(1001));
            r0.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("!!!!"));
            r0.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(-19890));
            
            RowPrimaryKey r1 = new RowPrimaryKey();
            r1.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("数据验证符合预期"));
            r1.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r2 = new RowPrimaryKey();
            r2.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("!!!!!!!!!!!!!"));
            r2.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r3 = new RowPrimaryKey();
            r3.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MIN);
            r3.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(-21312));
            r3.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("$$$$"));
            r3.addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(11887));
            
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r0, ranges.get(0).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(0).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(1).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(1).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(2).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r3, ranges.get(2).getEnd()) == 0);
        }
    }
    
    // =========================================================================
    // 4.验证边界字符串的切分
    // ========================================================================= 
    @Test
    public void testBoundaryStringTest() {
        TableMeta meta = new TableMeta("test");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
        meta.addPrimaryKeyColumn("pk4", PrimaryKeyType.INTEGER);
        
        OTSConf conf = new OTSConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        
        conf.setRangeBegin(rangeBegin);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeBegin()));
        range.setEnd(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeEnd()));
        
        {
            List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();

            splits.add(PrimaryKeyValue.INF_MIN);
            splits.add(PrimaryKeyValue.fromString(""));
            splits.add(PrimaryKeyValue.fromString("爱国和公安嘎斯噶的方式噶风格"));
            splits.add(PrimaryKeyValue.INF_MAX);
            
            List<OTSRange> ranges = RangeSplitUtils.specifySplitRange(meta, conf, range, splits);
            
            assertEquals(3, ranges.size());
            
            RowPrimaryKey r0 = new RowPrimaryKey();
            r0.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MIN);
            r0.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r0.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r0.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r1 = new RowPrimaryKey();
            r1.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(""));
            r1.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r2 = new RowPrimaryKey();
            r2.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("爱国和公安嘎斯噶的方式噶风格"));
            r2.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r3 = new RowPrimaryKey();
            r3.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MAX);
            r3.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MAX);
            r3.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MAX);
            r3.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MAX);
            
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r0, ranges.get(0).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(0).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(1).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(1).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(2).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r3, ranges.get(2).getEnd()) == 0);
        }
    }
   
    // =========================================================================
    // 5.验证边界数值型的切分
    // =========================================================================
    @Test
    public void testBoundaryNumberTest() {
        TableMeta meta = new TableMeta("test");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.STRING);
        meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk4", PrimaryKeyType.STRING);

        OTSConf conf = new OTSConf();
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromLong(Long.MIN_VALUE));
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        
        conf.setRangeBegin(rangeBegin);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeBegin()));
        range.setEnd(CommonUtils.checkInputPrimaryKeyAndGet(meta, conf.getRangeEnd()));
        
        {
            List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();

            splits.add(PrimaryKeyValue.INF_MIN);
            splits.add(PrimaryKeyValue.fromLong(-11111111111L));
            splits.add(PrimaryKeyValue.fromLong(-111111L));
            splits.add(PrimaryKeyValue.fromLong(-232));
            splits.add(PrimaryKeyValue.fromLong(-1));
            splits.add(PrimaryKeyValue.fromLong(0));
            splits.add(PrimaryKeyValue.fromLong(12332L));
            splits.add(PrimaryKeyValue.fromLong(13352L));
            splits.add(PrimaryKeyValue.fromLong(535333L));
            splits.add(PrimaryKeyValue.fromLong(Long.MAX_VALUE));
            
            List<OTSRange> ranges = RangeSplitUtils.specifySplitRange(meta, conf, range, splits);
            
            assertEquals(10, ranges.size());
            
            RowPrimaryKey r0 = new RowPrimaryKey();
            r0.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(Long.MIN_VALUE));
            r0.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MAX);
            r0.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MAX);
            r0.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MAX);
            
            RowPrimaryKey r1 = new RowPrimaryKey();
            r1.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(-11111111111L));
            r1.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r1.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r2 = new RowPrimaryKey();
            r2.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(-111111L));
            r2.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r2.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r3 = new RowPrimaryKey();
            r3.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(-232));
            r3.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r3.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r3.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r4 = new RowPrimaryKey();
            r4.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(-1));
            r4.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r4.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r4.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r5 = new RowPrimaryKey();
            r5.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(0));
            r5.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r5.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r5.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r6 = new RowPrimaryKey();
            r6.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(12332L));
            r6.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r6.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r6.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r7 = new RowPrimaryKey();
            r7.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(13352L));
            r7.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r7.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r7.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r8 = new RowPrimaryKey();
            r8.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(535333L));
            r8.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r8.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r8.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r9 = new RowPrimaryKey();
            r9.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(Long.MAX_VALUE));
            r9.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r9.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r9.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            RowPrimaryKey r10 = new RowPrimaryKey();
            r10.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MAX);
            r10.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
            r10.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
            r10.addPrimaryKeyColumn("pk4", PrimaryKeyValue.INF_MIN);
            
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r0, ranges.get(0).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(0).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r1, ranges.get(1).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(1).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r2, ranges.get(2).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r3, ranges.get(2).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r3, ranges.get(3).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r4, ranges.get(3).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r4, ranges.get(4).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r5, ranges.get(4).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r5, ranges.get(5).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r6, ranges.get(5).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r6, ranges.get(6).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r7, ranges.get(6).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r7, ranges.get(7).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r8, ranges.get(7).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r8, ranges.get(8).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r9, ranges.get(8).getEnd()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r9, ranges.get(9).getBegin()) == 0);
            assertEquals(true, CommonUtils.compareRangeBeginAndEnd(meta, r10, ranges.get(9).getEnd()) == 0);
        }
    }
}
