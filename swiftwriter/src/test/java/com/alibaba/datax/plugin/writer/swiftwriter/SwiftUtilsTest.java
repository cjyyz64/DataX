package com.alibaba.datax.plugin.writer.swiftwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.search.swift.SwiftClient;
import com.alibaba.search.swift.exception.SwiftException;
import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import static com.alibaba.datax.plugin.writer.swiftwriter.SwiftUtils.CMD_SEPARATOR;
import static com.alibaba.datax.plugin.writer.swiftwriter.SwiftUtils.FIELD_SEPARATOR;

/**
 * Created by zw86077 on 2015/12/9.
 */
public class SwiftUtilsTest extends TestCase {



    @Test
    public void testRecord2Doc() {

        Record r = new DefaultRecord();
        String uuid = "11111111";
        String request_time = "2015-12-09 15:58;20";
        String host = "www.taobao.com";
        String uri = "/index.html";
        String method = "GET";
        r.addColumn(new StringColumn(uuid));
        r.addColumn(new StringColumn(request_time));
        r.addColumn(new StringColumn(host));
        r.addColumn(new StringColumn(uri));
        r.addColumn(new StringColumn(method));
        String s = SwiftUtils.record2Doc(r, Lists.newArrayList("uuid", "request_time", "host", "uri", "method"));
        String expect = "CMD=add" + FIELD_SEPARATOR +
                "uuid=" + uuid + FIELD_SEPARATOR +
                "request_time=" + request_time + FIELD_SEPARATOR +
                "host=" + host + FIELD_SEPARATOR +
                "uri=" + uri + FIELD_SEPARATOR +
                "method=" + method + FIELD_SEPARATOR +
                CMD_SEPARATOR;
        assertEquals(expect, s);

    }




    public void testCreateTopicIfNotExists() throws SwiftException {
        SwiftClient swiftClient = new SwiftClient();
        swiftClient.init("zkPath=zfs://10.218.144.48:2181/swift"
                + ";logConfigFile=swift_alog.conf");

        SwiftUtils.checkTopicExists(swiftClient, "datax_test", 2);
    }



    @Test
    public void testParseTopicName() {
        String writerConfig = "topicName=datax_test;functionChain=HASH,hashId2partId";
        Assert.assertEquals("datax_test", SwiftUtils.extractTopicNameFromWriterConfig(writerConfig));
    }



}
