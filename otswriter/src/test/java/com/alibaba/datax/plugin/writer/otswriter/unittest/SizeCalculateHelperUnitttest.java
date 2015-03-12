package com.alibaba.datax.plugin.writer.otswriter.unittest;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.Pair;
import com.alibaba.datax.plugin.writer.otswriter.utils.SizeCalculateHelper;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowPrimaryKey;

public class SizeCalculateHelperUnitttest {

    @Test
    public void testGetPrimaryKeyValueSize() {
        assertEquals(10, SizeCalculateHelper.getPrimaryKeyValueSize(PrimaryKeyValue.fromString("0123456789")));
        assertEquals(8, SizeCalculateHelper.getPrimaryKeyValueSize(PrimaryKeyValue.fromLong(-100)));
    }
    
    @Test
    public void testGetColumnValueSize() throws UnsupportedEncodingException {
        assertEquals(10, SizeCalculateHelper.getColumnValueSize(ColumnValue.fromString("0123456789")));
        assertEquals(8, SizeCalculateHelper.getColumnValueSize(ColumnValue.fromLong(0)));
        assertEquals(8, SizeCalculateHelper.getColumnValueSize(ColumnValue.fromDouble(0)));
        assertEquals(1, SizeCalculateHelper.getColumnValueSize(ColumnValue.fromBoolean(true)));
        assertEquals(1, SizeCalculateHelper.getColumnValueSize(ColumnValue.fromBoolean(false)));
        assertEquals(6, SizeCalculateHelper.getColumnValueSize(ColumnValue.fromBinary("中国".getBytes("UTF-8"))));
    }
    
    @Test
    public void testGetRowPrimaryKeySize() {
        RowPrimaryKey pk = new RowPrimaryKey();
        pk.addPrimaryKeyColumn("hello", PrimaryKeyValue.fromString("0123456789"));// 15
        pk.addPrimaryKeyColumn("world", PrimaryKeyValue.fromString("0123456789"));// 15
        pk.addPrimaryKeyColumn("AAAAA", PrimaryKeyValue.fromLong(-1));// 13
        pk.addPrimaryKeyColumn("BBBBB", PrimaryKeyValue.fromLong(0));// 13
        pk.addPrimaryKeyColumn("CCCCC", PrimaryKeyValue.fromLong(1));// 13
        
        assertEquals(69, SizeCalculateHelper.getRowPrimaryKeySize(pk));
    }
    
    @Test
    public void testGetAttributeColumnSize() throws UnsupportedEncodingException {
        List<Pair<String, ColumnValue>> attr = new ArrayList<Pair<String, ColumnValue>>();
        attr.add(new Pair<String, ColumnValue>("col_0", ColumnValue.fromString("0123456789"))); // 15
        attr.add(new Pair<String, ColumnValue>("col_1", ColumnValue.fromLong(1000))); // 13
        attr.add(new Pair<String, ColumnValue>("col_2", ColumnValue.fromDouble(1.0))); // 13
        attr.add(new Pair<String, ColumnValue>("col_3", ColumnValue.fromBoolean(true))); // 6
        attr.add(new Pair<String, ColumnValue>("col_4", ColumnValue.fromBoolean(false))); // 6
        attr.add(new Pair<String, ColumnValue>("col_5", ColumnValue.fromBinary("ABC".getBytes("UTF-8")))); // 8
        attr.add(new Pair<String, ColumnValue>("col_6", null)); // 0
        
        assertEquals(61, SizeCalculateHelper.getAttributeColumnSize(attr, OTSOpType.PUT_ROW));
        assertEquals(66, SizeCalculateHelper.getAttributeColumnSize(attr, OTSOpType.UPDATE_ROW));
    }
    
}
