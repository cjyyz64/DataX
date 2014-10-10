package com.alibaba.datax.plugin.reader.otsreader.unittest;

import static org.junit.Assert.*;

import org.junit.Test;

import com.alibaba.datax.plugin.reader.otsreader.utils.Common;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

public class CommonUnittest {

    /**
     * 测试PrimaryKeyValue的大小比较
     */
    @Test
    public void testPrimaryKeyValuCmp() {
        // 不同类型，v1 = v2
        {
            assertTrue(0 == Common.primaryKeyValueCmp(PrimaryKeyValue.fromLong(10), PrimaryKeyValue.fromLong(10)));
            assertTrue(0 == Common.primaryKeyValueCmp(PrimaryKeyValue.fromString("hello"), PrimaryKeyValue.fromString("hello")));
            assertTrue(0 == Common.primaryKeyValueCmp(PrimaryKeyValue.INF_MIN, PrimaryKeyValue.INF_MIN));
            assertTrue(0 == Common.primaryKeyValueCmp(PrimaryKeyValue.INF_MAX, PrimaryKeyValue.INF_MAX));
        }
        
        // 不同类型，v1 < v2
        {
            assertTrue(0 > Common.primaryKeyValueCmp(PrimaryKeyValue.fromLong(10), PrimaryKeyValue.fromLong(11)));
            assertTrue(0 > Common.primaryKeyValueCmp(PrimaryKeyValue.fromString("a"), PrimaryKeyValue.fromString("hello")));
            assertTrue(0 > Common.primaryKeyValueCmp(PrimaryKeyValue.INF_MIN, PrimaryKeyValue.INF_MAX));
        }
        // 不同类型，v1 > v2
        {
            assertTrue(0 < Common.primaryKeyValueCmp(PrimaryKeyValue.fromLong(100), PrimaryKeyValue.fromLong(11)));
            assertTrue(0 < Common.primaryKeyValueCmp(PrimaryKeyValue.fromString("z"), PrimaryKeyValue.fromString("b")));
            assertTrue(0 < Common.primaryKeyValueCmp(PrimaryKeyValue.INF_MAX, PrimaryKeyValue.INF_MIN));
        }
        
        // 类型不一致
        {
            try {
                Common.primaryKeyValueCmp(PrimaryKeyValue.fromLong(100), PrimaryKeyValue.fromString("b"));
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }

            assertTrue(0 < Common.primaryKeyValueCmp(PrimaryKeyValue.fromLong(10), PrimaryKeyValue.INF_MIN));
            assertTrue(0 > Common.primaryKeyValueCmp(PrimaryKeyValue.fromLong(10), PrimaryKeyValue.INF_MAX));
            assertTrue(0 < Common.primaryKeyValueCmp(PrimaryKeyValue.fromString("z"), PrimaryKeyValue.INF_MIN));
            assertTrue(0 > Common.primaryKeyValueCmp(PrimaryKeyValue.fromString("z"), PrimaryKeyValue.INF_MAX));
        }
        // 边界测试
        {
            assertTrue(0 > Common.primaryKeyValueCmp(PrimaryKeyValue.fromLong(-1111111111), PrimaryKeyValue.INF_MAX));
            assertTrue(0 > Common.primaryKeyValueCmp(PrimaryKeyValue.fromLong(-1111111111), PrimaryKeyValue.fromLong(Long.MAX_VALUE)));
            assertTrue(0 < Common.primaryKeyValueCmp(PrimaryKeyValue.fromLong(0), PrimaryKeyValue.fromLong(Long.MIN_VALUE)));
            assertTrue(0 > Common.primaryKeyValueCmp(PrimaryKeyValue.fromLong(0), PrimaryKeyValue.fromLong(Long.MAX_VALUE)));
            assertTrue(0 < Common.primaryKeyValueCmp(PrimaryKeyValue.fromLong(11111111023423L), PrimaryKeyValue.fromLong(-11111111023423L)));
            assertTrue(0 < Common.primaryKeyValueCmp(PrimaryKeyValue.fromLong(Long.MAX_VALUE), PrimaryKeyValue.fromLong(Long.MIN_VALUE)));
            assertTrue(0 > Common.primaryKeyValueCmp(PrimaryKeyValue.fromLong(Long.MIN_VALUE), PrimaryKeyValue.fromLong(Long.MAX_VALUE)));
            
            assertTrue(0 > Common.primaryKeyValueCmp(PrimaryKeyValue.fromString(""), PrimaryKeyValue.fromString("数据验证符合预期")));
            assertTrue(0 < Common.primaryKeyValueCmp(PrimaryKeyValue.fromString("A"), PrimaryKeyValue.fromString("")));
            assertTrue(0 == Common.primaryKeyValueCmp(PrimaryKeyValue.fromString(""), PrimaryKeyValue.fromString("")));
            assertTrue(0 > Common.primaryKeyValueCmp(PrimaryKeyValue.fromString("!!!!!!!!!!!!!!!"), PrimaryKeyValue.fromString("还伴随着update_table调小CU导致无限次重试")));
        }

    }
}
