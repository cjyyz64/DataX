package com.alibaba.datax.plugin.reader.otsreader;

public class Constant {
    /**
     * Json中的Key名字定义
     */
    public class KEY {
        public static final String CONF = "conf";
        public static final String RANGE = "range";
        
        public static final String TIME_RANGE = "timeRange";
        public static final String MAX_VERSION = "maxVersion";
        public static final String RETRY = "maxRetryTime";
        public final static String SLEEP_IN_MILLISECOND = "retrySleepInMillisecond";
        public final static String IO_THREAD_COUNT = "ioThreadCount";
        public final static String MAX_CONNECT_COUNT = "maxConnectCount";
        public final static String SOCKET_TIMEOUTIN_MILLISECOND = "socketTimeoutInMillisecond";
        public final static String CONNECT_TIMEOUT_IN_MILLISECOND = "connectTimeoutInMillisecond";

        public class Range {
            public static final String BEGIN = "begin";
            public static final String END = "end";
            public static final String SPLIT = "split";
        };

        public class PrimaryKeyColumn {
            public static final String TYPE = "type";
            public static final String VALUE = "value";
        };
        
        public class Column {
            public static final String NAME = "name";
            public static final String TYPE = "type";
            public static final String VALUE = "value";
        };
        
        public class TimeRange {
            public static final String BEGIN = "begin";
            public static final String END = "end";
        }
    };
    
    /**
     * 全局的常量定义
     */
    public class VALUE {
        public static final int RETRY = 18;
        public static final int SLEEP_IN_MILLISECOND = 100;
        public final static int IO_THREAD_COUNT = 1;
        public final static int MAX_CONNECT_COUNT = 1;
        public final static int SOCKET_TIMEOUTIN_MILLISECOND = 10000;
        public final static int CONNECT_TIMEOUT_IN_MILLISECOND = 10000;
        
        public final static int MAX_VERSION = -1;

        public static final String DEFAULT_NAME = "DEFAULT_NAME";
        
        public class PrimaryKeyColumnType {
            public static final String INF_MIN = "INF_MIN";
            public static final String INF_MAX = "INF_MAX";
            public static final String STRING = "string";
            public static final String INTEGER = "int";
            public static final String BINARY = "binary";
        }
        
        public class ColumnType {
            public static final String STRING = "string";
            public static final String INTEGER = "int";
            public static final String DOUBLE = "double";
            public static final String BOOLEAN = "bool";
            public static final String BINARY = "binary";
        }
        
        public class TimeRange {
            public static final long MIN = 0;
            public static final long MAX = Long.MAX_VALUE;
        }
    }
}
