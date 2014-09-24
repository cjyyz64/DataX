package com.alibaba.datax.plugin.reader.otsreader.unittest;

import static org.junit.Assert.*;

import org.junit.Test;

import com.alibaba.datax.plugin.reader.otsreader.utils.CommonUtils;
import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

public class CommonUtilsUnittest {

    @Test
    public void testPrimaryKeyValuCmp() {
        // 不同类型，v1 = v2
        {
            assertTrue(0 == CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromLong(10), PrimaryKeyValue.fromLong(10)));
            assertTrue(0 == CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromString("hello"), PrimaryKeyValue.fromString("hello")));
            assertTrue(0 == CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.INF_MIN, PrimaryKeyValue.INF_MIN));
            assertTrue(0 == CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.INF_MAX, PrimaryKeyValue.INF_MAX));
        }
        // 不同类型，v1 < v2
        {
            assertTrue(0 > CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromLong(10), PrimaryKeyValue.fromLong(11)));
            assertTrue(0 > CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromString("a"), PrimaryKeyValue.fromString("hello")));
            assertTrue(0 > CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.INF_MIN, PrimaryKeyValue.INF_MAX));
        }
        // 不同类型，v1 > v2
        {
            assertTrue(0 < CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromLong(100), PrimaryKeyValue.fromLong(11)));
            assertTrue(0 < CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromString("z"), PrimaryKeyValue.fromString("b")));
            assertTrue(0 < CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.INF_MAX, PrimaryKeyValue.INF_MIN));
        }
        // 类型不一致
        {
            try {
                CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromLong(100), PrimaryKeyValue.fromString("b"));
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }

            assertTrue(0 < CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromLong(10), PrimaryKeyValue.INF_MIN));
            assertTrue(0 > CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromLong(10), PrimaryKeyValue.INF_MAX));
            assertTrue(0 < CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromString("z"), PrimaryKeyValue.INF_MIN));
            assertTrue(0 > CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromString("z"), PrimaryKeyValue.INF_MAX));
        }
        // 边界测试
        {
            assertTrue(0 > CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromLong(-1111111111), PrimaryKeyValue.INF_MAX));
            assertTrue(0 > CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromLong(-1111111111), PrimaryKeyValue.fromLong(Long.MAX_VALUE)));
            assertTrue(0 < CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromLong(0), PrimaryKeyValue.fromLong(Long.MIN_VALUE)));
            assertTrue(0 > CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromLong(0), PrimaryKeyValue.fromLong(Long.MAX_VALUE)));
            assertTrue(0 < CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromLong(11111111023423L), PrimaryKeyValue.fromLong(-11111111023423L)));
            assertTrue(0 < CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromLong(Long.MAX_VALUE), PrimaryKeyValue.fromLong(Long.MIN_VALUE)));
            assertTrue(0 > CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromLong(Long.MIN_VALUE), PrimaryKeyValue.fromLong(Long.MAX_VALUE)));
            
            assertTrue(0 > CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromString(""), PrimaryKeyValue.fromString("数据验证符合预期")));
            assertTrue(0 < CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromString("A"), PrimaryKeyValue.fromString("")));
            assertTrue(0 == CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromString(""), PrimaryKeyValue.fromString("")));
            assertTrue(0 > CommonUtils.primaryKeyValueCmp(PrimaryKeyValue.fromString("!!!!!!!!!!!!!!!"), PrimaryKeyValue.fromString("还伴随着update_table调小CU导致无限次重试")));
        }

    }

    @Test
    public void testGetRetryTimes() throws Exception {
        OTSException e = null;
        // 无限重试
        {
            int remainingRetryTimes = 10;
            e = new OTSException("", null, OTSErrorCode.SERVER_BUSY, "");
            assertEquals(10, CommonUtils.getRetryTimes(e, remainingRetryTimes));

            e = new OTSException("", null, OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "");
            assertEquals(10, CommonUtils.getRetryTimes(e, remainingRetryTimes));

            e = new OTSException("", null, OTSErrorCode.TABLE_NOT_READY, "");
            assertEquals(10, CommonUtils.getRetryTimes(e, remainingRetryTimes));
        }

        // 有限重试
        {
            int remainingRetryTimes = 10;
            e = new OTSException("", null, OTSErrorCode.INTERNAL_SERVER_ERROR, "");
            assertEquals(9, CommonUtils.getRetryTimes(e, remainingRetryTimes));

            e = new OTSException("", null, OTSErrorCode.REQUEST_TIMEOUT, "");
            assertEquals(9, CommonUtils.getRetryTimes(e, remainingRetryTimes));

            e = new OTSException("", null, OTSErrorCode.PARTITION_UNAVAILABLE, "");
            assertEquals(9, CommonUtils.getRetryTimes(e, remainingRetryTimes));

            e = new OTSException("", null, OTSErrorCode.STORAGE_TIMEOUT, "");
            assertEquals(9, CommonUtils.getRetryTimes(e, remainingRetryTimes));

            e = new OTSException("", null, OTSErrorCode.SERVER_UNAVAILABLE, "");
            assertEquals(9, CommonUtils.getRetryTimes(e, remainingRetryTimes));
        }
        // 不能重试
        {
            int remainingRetryTimes = 10;

            try {
                e = new OTSException("", null, OTSErrorCode.AUTHORIZATION_FAILURE, "");
                CommonUtils.getRetryTimes(e, remainingRetryTimes);
                assertTrue(false);
            } catch (Exception ee){
                assertTrue(true);
            }
            try {
                e = new OTSException("", null, OTSErrorCode.INVALID_PARAMETER, "");
                CommonUtils.getRetryTimes(e, remainingRetryTimes);
                assertTrue(false);
            } catch (Exception ee){
                assertTrue(true);
            }
            try {
                e = new OTSException("", null, OTSErrorCode.REQUEST_TOO_LARGE, "");
                CommonUtils.getRetryTimes(e, remainingRetryTimes);
                assertTrue(false);
            } catch (Exception ee){
                assertTrue(true);
            }
            try {
                e = new OTSException("", null, OTSErrorCode.OBJECT_NOT_EXIST, "");
                CommonUtils.getRetryTimes(e, remainingRetryTimes);
                assertTrue(false);
            } catch (Exception ee){
                assertTrue(true);
            }
            try {
                e = new OTSException("", null, OTSErrorCode.INVALID_PK, "");
                CommonUtils.getRetryTimes(e, remainingRetryTimes);
                assertTrue(false);
            } catch (Exception ee){
                assertTrue(true);
            }
        }        

}
}
