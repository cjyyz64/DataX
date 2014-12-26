package com.alibaba.datax.plugin.reader.hbasereader.util;

import ch.qos.logback.classic.Level;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogUtil {

    private static final String LOG_LEVEL_FLAG = "logLevel";

    public static class ReaderLog {
        private static String readerLogLevel;

        // 只调用一次
        public static void initLoglevel(Class clazz, Configuration configuration) {
            readerLogLevel = LogUtil.getLoglevel(configuration);
            LogUtil.getLogger(clazz, readerLogLevel);
        }

        public static Logger getLogger(Class clazz) {
            return LogUtil.getLogger(clazz, readerLogLevel);
        }
    }

    public static class WriterLog {
        private static String writerLogLevel;

        // 只调用一次
        public static void initLoglevel(Class clazz, Configuration configuration) {
            writerLogLevel = LogUtil.getLoglevel(configuration);
            LogUtil.getLogger(clazz, writerLogLevel);
        }

        public static Logger getLogger(Class clazz) {
            return LogUtil.getLogger(clazz, writerLogLevel);
        }
    }


    private LogUtil() {
    }

    // 最佳实践，只调用一次
    private static String getLoglevel(Configuration configuration) {
        String level = configuration.getString(LOG_LEVEL_FLAG);
        if (StringUtils.isNotBlank(level)) {
            return level;
        }
        return null;
    }

    private static void adjustLogLevel(Logger logger, String logLevel) {
        if (StringUtils.isBlank(logLevel)) {
            return;
        }

        if (logger instanceof ch.qos.logback.classic.Logger) {
            ((ch.qos.logback.classic.Logger) logger).setLevel(Level.toLevel(logLevel));
        }
    }

    private static Logger getLogger(Class clazz, String logLevel) {
        Logger logger = LoggerFactory.getLogger(clazz);
        adjustLogLevel(logger, logLevel);
        return logger;
    }

}
