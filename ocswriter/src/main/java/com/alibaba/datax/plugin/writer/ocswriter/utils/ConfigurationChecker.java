package com.alibaba.datax.plugin.writer.ocswriter.utils;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.ocswriter.Key;
import com.google.common.base.Preconditions;
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

    private enum WRITE_MODE {
        set
    }

    private enum WRITE_FORMAT {
        text
    }

    /**
     * 参数有效性基本检查
     */
    private static void paramCheck(Configuration config) {
        String proxy = config.getString(Key.PROXY);
        Preconditions.checkArgument(StringUtils.isNoneBlank(proxy), "proxy of ocs could not be blank");

        String port = config.getString(Key.PORT, "11211");
        Preconditions.checkArgument(StringUtils.isNoneBlank(port), "port of ocs could not be blank");

        String userName = config.getString(Key.USER);
        Preconditions.checkArgument(StringUtils.isNoneBlank(userName), "user name could not be blank");

        String password = config.getString(Key.PASSWORD);
        Preconditions.checkArgument(StringUtils.isNoneBlank(password), "password could not be blank");

        String indexes = config.getString(Key.INDEXES, "0");
        Preconditions.checkArgument(StringUtils.isNoneBlank(indexes), "indexes could not be blank");
        for (String index : indexes.split(",")) {
            try {
                Preconditions.checkArgument(Integer.parseInt(index) >= 0, "index could not be less than zero");
            } catch (NumberFormatException e) {
                Preconditions.checkArgument(false, "illegal index");
            }
        }

        String writerMode = config.getString(Key.WRITE_MODE);
        Preconditions.checkArgument(EnumUtils.isValidEnum(WRITE_MODE.class, writerMode.toLowerCase()), String.format("not supported write mode:%s, recommended:%s", writerMode, StringUtils.join(WRITE_MODE.values(), ",")));

        String writerFormat = config.getString(Key.WRITE_FORMAT, "text");
        Preconditions.checkArgument(EnumUtils.isValidEnum(WRITE_MODE.class, writerMode.toLowerCase()), String.format("not supported write format:%s, recommended:%s", writerFormat, StringUtils.join(WRITE_FORMAT.values(), ",")));

        //fieldDelimiter不需要检查，默认为\u0001
        //expireTime不需要检查，默认为Integer.MAX_VALUE
        //batchSize默认为1000
    }

    /**
     * 检查ocs服务器网络是否可达
     */
    private static void hostReachableCheck(Configuration config) {
        String proxy = config.getString(Key.PROXY);
        try {
            boolean status = InetAddress.getByName(proxy).isReachable(10000);
            Preconditions.checkArgument(status, String.format("proxy:%s is not reachable", proxy));
        } catch (IOException e) {
            Preconditions.checkArgument(false, String.format("unknown host:%s", proxy));
        }
    }
}
