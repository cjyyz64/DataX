package com.alibaba.datax.plugin.writer.tairwriter;

public class Key {
    /*
     * @name: configId
     *
     * @description: destination tair cluster configId (you can got it in
     * http://tair.corp.taobao.com:9999)
     *
     * @range:
     *
     * @mandatory: true
     */
    public final static String CONFIG_ID = "configId";

    /*
     * @name: namespace
     *
     * @description: destination namespace
     *
     * @range: [1, 65565]
     *
     * @mandatory: true
     */
    public final static String NAMESPACE = "namespace";

    /*
     * @name: language
     *
     * @description: tair client to read or write after transfer finished
     *
     * @range: c++ java
     *
     * @mandatory: true
     */
    public final static String LANGUAGE = "language";

    /*
     * @name: writer_type
     *
     * @description:
     *
     * @range: put prefixput multiprefix counter prefixcounter multiprefixcounter
     *
     * @mandatory: true
     */
    public final static String WRITER_TYPE = "writerType";

    /*
     * @name: seprator
     *
     * @description: using seprator to split value [put mode]
     *
     * @range:
     *
     * @mandatory: false [true when put mode]
     *
     * @default: \t
     */
    public final static String FIELD_DELIMITER = "fieldDelimiter";

    /*
     * Advanced options: common user needn't care these option. modify them when
     * perf test, or specical cases.
     */

    /*
     * @name: COLUMN
     *
     * @description: indecate skey name list, please fill in column name
     *
     * @range: col2,col3..
     *
     * @mandatory: false [true when multiprefixput or multiprefixcounter mode]
     *
     */
    public final static String SKEY_LIST = "skeyList";

    /*
     * @name: expire time
     *
     * @description:key auto expire time (second), 0 mean never expire
     *
     * @range: 0 ~ oo
     *
     * @mandatory: true
     *
     */
    public final static String EXPIRE = "expire";

    /*
     * @name: CompressThreold
     *
     * @description: value's CompressionThreshold (bytes), if beyond this tairclient will compress
     *
     * @range: positive integer
     *
     * @mandatory: false [only java client need]
     *
     * @default: 8196
     */
    public final static String COMPRESSION_THRESHOLD = "compressionThreshold";

    /*
     * @name: Timeout
     *
     * @description: request to tair max timeout(ms), if operator beyond this tairclient will return timeout
     *
     * @range: positive integer
     *
     * @mandatory: false
     *
     * @default: 2000
     */
    public final static String TIMEOUT = "timeout";

    /*
     * @name: delete empty record from tair
     *
     * @description: if true we will convert null to a delete key operator. or just skip it as dirty.
     *
     * @range: true false
     *
     * @mandatory: false [only put mode can set true]
     *
     * @default: false
     */
    public final static String DELETE_EMPTY_RECORD = "deleteEmptyRecord";

    /*
     * @name: uniform front leading string for key or pkey
     *
     * @description: uniform front leading string for key or pkey, if config all key or pkey will prepend the string
     *
     * @range: non-empty
     *
     * @mandatory: false
     *
     * @default: empty string
     */
    public final static String FRONT_LEADING_KEY = "frontLeadingKey";

    public final static String TAIR_THREAD_NUM = "threadNum";

}
