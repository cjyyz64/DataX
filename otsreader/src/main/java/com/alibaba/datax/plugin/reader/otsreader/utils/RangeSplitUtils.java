package com.alibaba.datax.plugin.reader.otsreader.utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSPrimaryKeyColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.TableMeta;

/**
 * 主要提供对范围的解析
 */
public class RangeSplitUtils {

    /**
     * 该方法只支持beginStr小于endStr
     * 
     * @param beginStr
     * @param endStr
     * @param count
     * @return
     */
    private static List<String> splitCodePoint(int begin, int end, int count) {
        List<String> results = new ArrayList<String>();
        BigInteger beginBig = BigInteger.valueOf(begin);
        BigInteger endBig = BigInteger.valueOf(end);
        BigInteger countBig = BigInteger.valueOf(count);
        BigInteger multi = endBig.subtract(beginBig).add(BigInteger.ONE);
        BigInteger range = endBig.subtract(beginBig);
        BigInteger interval = BigInteger.ZERO;
        int length = 1;

        BigInteger tmpBegin =BigInteger.ZERO;
        BigInteger tmpEnd =  endBig.subtract(beginBig);

        // 扩大之后的数值
        BigInteger realBegin = tmpBegin; 
        BigInteger realEnd = tmpEnd;     

        while (range.compareTo(countBig) < 0) { // 不够切分
            realEnd   = realEnd.multiply(multi).add(tmpEnd);
            range = realEnd.subtract(realBegin);
            length++;
        }

        interval = range.divide(countBig);

        List<BigInteger> points = new ArrayList<BigInteger>();

        int index = 0;
        BigInteger cur = realBegin;
        
        while(index < count - 1) {
            points.add(cur);
            cur = cur.add(interval);
            index++;
        }
        points.add(realEnd);

        for (BigInteger v : points) {
            BigInteger tmp = v;
            StringBuffer sb = new StringBuffer();
            for (int tmpLength = 0; tmpLength < length; tmpLength++) {
                sb.insert(0, (char)(beginBig.add(tmp.remainder(multi)).intValue()));
                tmp = tmp.divide(multi);
            }
            results.add(sb.toString());
        }
        return results;
    }
    
    public static List<String> splitStringRange(String begin, String end, int count) {
        
        if (count <= 1) {
            throw new IllegalArgumentException("Input count <= 1 .") ;
        }
        
        List<String> results = new ArrayList<String>();
        
        int beginValue = 0;
        if (!begin.isEmpty()) {
            beginValue = begin.codePointAt(0);
        }
        int endValue = 0;
        if (!end.isEmpty()) {
            endValue = end.codePointAt(0);
        }
        
        if (beginValue < endValue) {
            List<String> tmp = splitCodePoint(beginValue, endValue, count);
            for (String value : tmp) {
                if (begin.compareTo(value) <= 0 && end.compareTo(value) >= 0 ) {
                    results.add(value);
                }
            }
            
        } else if (beginValue > endValue ){
            List<String> tmp = splitCodePoint(endValue, beginValue, count);
            // sort
            Collections.sort(tmp, new Comparator<String>() {
                public int compare(String arg0, String arg1) {
                    return  arg1.compareTo(arg0);
                }
            });
            for (String value : tmp) {
                if (begin.compareTo(value) >= 0 && end.compareTo(value) <= 0 ) {
                    results.add(value);
                }
            }
        } else {
            results.add(begin);
            results.add(end);
        }
        // replace first and last
        results.remove(0);
        results.add(0, begin);
        
        results.remove(results.size() - 1);
        results.add(end);
        
        return results;
    }
    
    /*
     * 根据count，平均分配[begin, end)
     */
    public static List<Long> splitIntegerRange(long begin, long end, int count){
        BigInteger bigBegin = BigInteger.valueOf(begin);
        BigInteger bigEnd = BigInteger.valueOf(end);
        BigInteger bigCount = BigInteger.valueOf(count);
       
        BigInteger abs = (bigEnd.subtract(bigBegin)).abs();
        
        List<Long> is = new ArrayList<Long>();
        
        if (abs.compareTo(BigInteger.ZERO) == 0) { // partition key 相等的情况
            is.add(bigBegin.longValue());
            is.add(bigEnd.longValue());
            return is;
        }
        
        if (bigCount.compareTo(abs) > 0) {
            bigCount = abs;
        }

        BigInteger interval = (bigEnd.subtract(bigBegin)).divide(bigCount);
        BigInteger cur = bigBegin;
        int i = 0;
        while(cur.compareTo(bigEnd) < 0 && i < count) {
            is.add(cur.longValue());
            cur = cur.add(interval);
            i++;
        }
        is.add(end);
        return is;
    }
    
    private static OTSPrimaryKeyColumn getPartitionKey(TableMeta meta) {
        List<String> keys = new ArrayList<String>();
        keys.addAll(meta.getPrimaryKey().keySet());
        
        String key = keys.get(0);
        
        OTSPrimaryKeyColumn col = new OTSPrimaryKeyColumn();
        col.setName(key);
        col.setType(meta.getPrimaryKey().get(key));
        return col;
    }
    
    public static List<OTSRange> splitRange(TableMeta meta, RowPrimaryKey begin, RowPrimaryKey end, int count) {
        List<OTSRange> results = new ArrayList<OTSRange>();
        
        OTSPrimaryKeyColumn partitionKey = getPartitionKey(meta);
        
        PrimaryKeyValue beginPartitionKey = begin.getPrimaryKey().get(partitionKey.getName());
        PrimaryKeyValue endPartitionKey = end.getPrimaryKey().get(partitionKey.getName());
        
        // 第一，先对PartitionKey列进行拆分
        
        if (partitionKey.getType() == PrimaryKeyType.INTEGER) {
            long beginLong = beginPartitionKey.asLong();
            long endLong = endPartitionKey.asLong();
            List<Long> ranges = RangeSplitUtils.splitIntegerRange(beginLong, endLong, count + 1);
            int size = ranges.size();
            for (int i = 0; i < size - 1; i++) {
                RowPrimaryKey bPk = new RowPrimaryKey();
                RowPrimaryKey ePk = new RowPrimaryKey();

                bPk.addPrimaryKeyColumn(partitionKey.getName(), PrimaryKeyValue.fromLong(ranges.get(i)));
                ePk.addPrimaryKeyColumn(partitionKey.getName(), PrimaryKeyValue.fromLong(ranges.get(i + 1)));
                
                results.add(new OTSRange(bPk, ePk));
            }
        } else {
            String beginString = beginPartitionKey.asString();
            String endString = endPartitionKey.asString();
            List<String> ranges = RangeSplitUtils.splitStringRange(beginString, endString, count + 1);
            int size = ranges.size();
            for (int i = 0; i < size - 1; i++) {
                RowPrimaryKey bPk = new RowPrimaryKey();
                RowPrimaryKey ePk = new RowPrimaryKey();

                bPk.addPrimaryKeyColumn(partitionKey.getName(), PrimaryKeyValue.fromString(ranges.get(i)));
                ePk.addPrimaryKeyColumn(partitionKey.getName(), PrimaryKeyValue.fromString(ranges.get(i + 1)));
                
                results.add(new OTSRange(bPk, ePk));
            }
        }
        
        // 第二，填充非PartitionKey的ParimaryKey列
        // 注意：在填充过程中，需要使用用户给定的Begin和End来替换切分出来的第一个Range
        //      的Begin和最后一个Range的End
        
        List<String> keys = new ArrayList<String>(meta.getPrimaryKey().size());
        keys.addAll(meta.getPrimaryKey().keySet());
        
        for (int i = 0; i < results.size(); i++) {
            for (int j = 1 ; j < keys.size(); j++) {
                OTSRange c = results.get(i);
                RowPrimaryKey beginPK = c.getBegin();
                RowPrimaryKey endPK = c.getEnd();
                String key = keys.get(j);
                if (i == 0) { // 第一行
                    beginPK.addPrimaryKeyColumn(key,  begin.getPrimaryKey().get(key));
                    endPK.addPrimaryKeyColumn(key,  PrimaryKeyValue.INF_MIN);
                } else if (i == results.size() - 1) {// 最后一行
                    beginPK.addPrimaryKeyColumn(key,  PrimaryKeyValue.INF_MIN);
                    endPK.addPrimaryKeyColumn(key,  end.getPrimaryKey().get(key));
                } else {
                    beginPK.addPrimaryKeyColumn(key,  PrimaryKeyValue.INF_MIN);
                    endPK.addPrimaryKeyColumn(key,  PrimaryKeyValue.INF_MIN);
                }
            }
        }
        return results;
    }
    
    private static List<PrimaryKeyValue> getCompletePK(int num, PrimaryKeyValue value) {
        List<PrimaryKeyValue> values = new ArrayList<PrimaryKeyValue>();
        for (int j = 0; j < num; j++) {
            if (j == 0) {
                values.add(value);
            } else {
                // 这里在填充PK时，系统需要选择特定的值填充于此
                // 系统默认填充INF_MIN
                values.add(PrimaryKeyValue.INF_MIN);
            }
        }
        return values;
    }
    
    public static List<OTSRange> specifySplitRange(TableMeta meta, OTSConf conf, OTSRange range, List<PrimaryKeyValue> splits) {
        List<OTSRange> results = new ArrayList<OTSRange>();

        int pkCount = meta.getPrimaryKey().size();

        PrimaryKeyValue begin = conf.getRangeBegin().get(0);
        PrimaryKeyValue end = conf.getRangeEnd().get(0);
        
        List<PrimaryKeyValue> newSplits = CommonUtils.getSplitPointByPartitionKey(begin, end, splits);
        
        for (int i = 0; i < newSplits.size() - 1; i ++) {
            OTSRange item = new OTSRange(
                    CommonUtils.checkInputPrimaryKeyAndGet(meta, getCompletePK(pkCount, newSplits.get(i))),
                    CommonUtils.checkInputPrimaryKeyAndGet(meta, getCompletePK(pkCount, newSplits.get(i + 1)))
                    );
            results.add(item);
        }
        // replace first and last
        OTSRange first = results.get(0);
        OTSRange last = results.get(results.size() - 1);
        
        first.setBegin(range.getBegin());
        last.setEnd(range.getEnd());
        return results;
    }
}
