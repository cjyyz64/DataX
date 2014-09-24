package com.alibaba.datax.plugin.reader.otsreader.unittest;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.alibaba.datax.plugin.reader.otsreader.utils.RangeSplitUtils;

/**
 * 测试功能点
 * http://wiki.aliyun-inc.com/projects/apsara/wiki/datax_ots_reader_testing
 * @author wanhong.chenwh@alibaba-inc.com
 *
 */
public class RangeSplitUtilsDefaultUnittest {

    // =========================================================================
    // 1.基础测试，测试切分功能正常，切分的范围符合预期
    // =========================================================================

    @Test
    public void testSplitStringRangeBase() {
        // 正序
        {
            List<String> points = RangeSplitUtils.splitStringRange("0", "9", 9);
            assertEquals(9, points.size());

            assertEquals("0", points.get(0));
            assertEquals("3", points.get(3));
            assertEquals("5", points.get(5));
            assertEquals("9", points.get(8));
        }
        // 反序
        {
            List<String> points = RangeSplitUtils.splitStringRange("9", "0", 9);
            assertEquals(9, points.size());

            assertEquals("9", points.get(0));
            assertEquals("5", points.get(3));
            assertEquals("3", points.get(5));
            assertEquals("0", points.get(8));
        }
        // 第一次不够切
        {
            List<String> points = RangeSplitUtils.splitStringRange("0", "5", 10);

            assertEquals(9, points.size());
            
            assertEquals("0", points.get(0));
            assertEquals("03", points.get(1));
            assertEquals("10", points.get(2));
            assertEquals("13", points.get(3));
            assertEquals("20", points.get(4));
            assertEquals("23", points.get(5));
            assertEquals("30", points.get(6));
            assertEquals("33", points.get(7));
            assertEquals("5", points.get(8));
        }
        // 切分很多份数
        {
            List<String> points = RangeSplitUtils.splitStringRange("", "阿里巴巴", 100001);

            assertEquals(100000, points.size());

            assertEquals("", points.get(0));
            assertEquals("阿里巴巴", points.get(99999));
        }
        
        // 切分很多份数
        {
            List<String> points = RangeSplitUtils.splitStringRange("5", "999", 10);
            
            assertEquals(10, points.size());
            
            assertEquals("5", points.get(0));
            assertEquals("57", points.get(1));
            assertEquals("59", points.get(2));
            assertEquals("66", points.get(3));
            assertEquals("68", points.get(4));
            assertEquals("75", points.get(5));
            assertEquals("77", points.get(6));
            assertEquals("79", points.get(7));
            assertEquals("86", points.get(8));
            assertEquals("999", points.get(9));
        }
    }

    @Test
    public void testSplitIntegerRangeBase() {
        List<Long> points = RangeSplitUtils.splitIntegerRange(0, 1000, 9);
        assertEquals(10, points.size());

        assertEquals(0, points.get(0).longValue());
        assertEquals(333, points.get(3).longValue());
        assertEquals(555, points.get(5).longValue());
        assertEquals(1000, points.get(9).longValue());
    }

    // =========================================================================
    // 2.基础测试，测试数值型不够切
    // =========================================================================

    @Test
    public void testSplitIntegerRangeBaseWithNoEnough() {
        List<Long> points = RangeSplitUtils.splitIntegerRange(0, 9000, 19999);
        assertEquals(9001, points.size());

        assertEquals(0, points.get(0).longValue());
        assertEquals(3, points.get(3).longValue());
        assertEquals(5, points.get(5).longValue());
        assertEquals(9, points.get(9).longValue());
        assertEquals(9000, points.get(9000).longValue());
    }

    // =========================================================================
    // 3.验证特殊字符串的切分（中文及中文符号，特殊符号）
    // =========================================================================
    @Test
    public void testSplitStringRangeWithSpecialChar() {
        // 特殊字符串切分，简单验证功能的正确性
        {
            List<String> points = RangeSplitUtils.splitStringRange("GGGG", "同步弹内外数据", 15);

            assertEquals(14, points.size());

            assertEquals("GGGG", points.get(0));
            // TODO 验证中间字段
            assertEquals("同步弹内外数据", points.get(13));
        }
        // 特殊字符串切分，简单验证功能的正确性
        {
            List<String> points = RangeSplitUtils.splitStringRange("00000", "!!!!@#$%^&*()", 20);

            assertEquals(19, points.size());

            assertEquals("00000", points.get(0));
            // TODO 验证中间字段
            assertEquals("!!!!@#$%^&*()", points.get(18));
        }
    }

    // =========================================================================
    // 4.验证边界数值型的切分 （Long.MIN_VALUE, Long.MAX_VALUE）
    // =========================================================================
    @Test
    public void testSplitIntegerRangeWithMinAndMax() {
        // 边界测试，简单验证功能的正确性
        {
            List<Long> points = RangeSplitUtils.splitIntegerRange(Long.MIN_VALUE, Long.MAX_VALUE, 4);

            assertEquals(5, points.size());

            assertEquals(Long.MIN_VALUE, points.get(0).longValue());
            assertEquals(-4611686018427387905L, points.get(1).longValue());
            assertEquals(-2, points.get(2).longValue());
            assertEquals(4611686018427387901L, points.get(3).longValue());
            assertEquals(Long.MAX_VALUE, points.get(4).longValue());
        }
    }
}
