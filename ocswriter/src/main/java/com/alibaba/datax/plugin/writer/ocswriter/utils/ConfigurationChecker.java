package com.alibaba.datax.plugin.writer.ocswriter.utils;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.ocswriter.Key;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Time:    2015-05-07 16:48
 * Creator: yuanqi@alibaba-inc.com
 */
public class ConfigurationChecker {

    public static void check(Configuration config) {
        paramCheck(config);
        hostReachableCheck(config);
    }

    public enum WRITE_MODE {
        set,
        add,
        replace,
        append,
        prepend
    }

    private enum WRITE_FORMAT {
        text
    }

    /**
     * 参数有效性基本检查
     */
    private static void paramCheck(Configuration config) {
        String proxy = config.getString(Key.PROXY);
        if (StringUtils.isBlank(proxy)) {
            throw DataXException.asDataXException(OcsWriterErrorCode.REQUIRED_VALUE, String.format("ocs服务地址%s不能设置为空", Key.PROXY));
        }
        String user = config.getString(Key.USER);
        if (StringUtils.isBlank(user)) {
            throw DataXException.asDataXException(OcsWriterErrorCode.REQUIRED_VALUE, String.format("访问ocs的用户%s不能设置为空", Key.USER));
        }
        String password = config.getString(Key.PASSWORD);
        if (StringUtils.isBlank(password)) {
            throw DataXException.asDataXException(OcsWriterErrorCode.REQUIRED_VALUE, String.format("访问ocs的用户%s不能设置为空", Key.PASSWORD));
        }

        String port = config.getString(Key.PORT, "11211");
        if (StringUtils.isBlank(port)) {
            throw DataXException.asDataXException(OcsWriterErrorCode.REQUIRED_VALUE, String.format("ocs端口%s不能设置为空", Key.PORT));
        }

        String indexes = config.getString(Key.INDEXES, "0");
        if (StringUtils.isBlank(indexes)) {
            throw DataXException.asDataXException(OcsWriterErrorCode.REQUIRED_VALUE, String.format("当做key的列编号%s不能为空", Key.INDEXES));
        }
        for (String index : indexes.split(",")) {
            try {
                if (Integer.parseInt(index) < 0) {
                    throw DataXException.asDataXException(OcsWriterErrorCode.ILLEGAL_PARAM_VALUE, String.format("列编号%s必须为逗号分隔的非负整数", Key.INDEXES));
                }
            } catch (NumberFormatException e) {
                throw DataXException.asDataXException(OcsWriterErrorCode.ILLEGAL_PARAM_VALUE, String.format("列编号%s必须为逗号分隔的非负整数", Key.INDEXES));
            }
        }

        String writerMode = config.getString(Key.WRITE_MODE);
        if (StringUtils.isBlank(writerMode)) {
            throw DataXException.asDataXException(OcsWriterErrorCode.REQUIRED_VALUE, String.format("操作方式%s不能为空", Key.WRITE_MODE));
        }
        if (!EnumUtils.isValidEnum(WRITE_MODE.class, writerMode.toLowerCase())) {
            throw DataXException.asDataXException(OcsWriterErrorCode.ILLEGAL_PARAM_VALUE, String.format("不支持操作方式%s，仅支持%s", writerMode, StringUtils.join(WRITE_MODE.values(), ",")));
        }

        String writerFormat = config.getString(Key.WRITE_FORMAT, "text");
        if (StringUtils.isBlank(writerFormat)) {
            throw DataXException.asDataXException(OcsWriterErrorCode.REQUIRED_VALUE, String.format("写入格式%s不能为空", Key.WRITE_FORMAT));
        }
        if (!EnumUtils.isValidEnum(WRITE_FORMAT.class, writerFormat.toLowerCase())) {
            throw DataXException.asDataXException(OcsWriterErrorCode.ILLEGAL_PARAM_VALUE, String.format("不支持写入格式%s，仅支持%s", writerFormat, StringUtils.join(WRITE_FORMAT.values(), ",")));
        }

        int expireTime = config.getInt(Key.EXPIRE_TIME, 0);
        if (expireTime < 0) {
            throw DataXException.asDataXException(OcsWriterErrorCode.ILLEGAL_PARAM_VALUE, String.format("数据过期时间设置%s不能小于0", Key.EXPIRE_TIME));
        }

        int batchSiz = config.getInt(Key.BATCH_SIZE, 100);
        if (batchSiz <= 0) {
            throw DataXException.asDataXException(OcsWriterErrorCode.ILLEGAL_PARAM_VALUE, String.format("批量写入大小设置%s必须大于0", Key.BATCH_SIZE));
        }
        //fieldDelimiter不需要检查，默认为\u0001
    }

    /**
     * 检查ocs服务器网络是否可达
     */
    private static void hostReachableCheck(Configuration config) {
        String proxy = config.getString(Key.PROXY);
        try {
            boolean status = InetAddress.getByName(proxy).isReachable(10000);
            if (!status) {
                throw DataXException.asDataXException(OcsWriterErrorCode.HOST_UNREACHABLE, String.format("主机%s网络不可达", proxy));
            }
        } catch (IOException e) {
            throw DataXException.asDataXException(OcsWriterErrorCode.HOST_UNREACHABLE, String.format("不存在的host地址:%s", proxy));
        }
    }

    /**
     * 以下为测试使用
     */
    @VisibleForTesting
    public static void paramCheck_test(Configuration configuration) {
        paramCheck(configuration);
    }

    @VisibleForTesting
    public static void hostReachableCheck_test(Configuration configuration) {
        hostReachableCheck(configuration);
    }
}
