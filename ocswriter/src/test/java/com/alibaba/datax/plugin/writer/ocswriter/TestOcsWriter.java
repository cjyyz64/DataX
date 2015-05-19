package com.alibaba.datax.plugin.writer.ocswriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.plugin.writer.ocswriter.utils.CommonUtils;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import org.easymock.EasyMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

/**
 * Time:    2015-05-06 19:26
 * Creator: yuanqi@alibaba-inc.com
 */
@Test
public class TestOcsWriter {

    Logger logger = LoggerFactory.getLogger(TestOcsWriter.class);

    MemcachedClient client = null;
    String username = "4aae568b4ff543d2";
    String password = "ocsPassword_123";
    String proxy = "10.232.4.25";
    String port = "11211";

    OcsWriter.Task task;

    @BeforeClass
    public void setup() {
//
//        AuthDescriptor ad = new AuthDescriptor(new String[]{"PLAIN"}, new PlainCallbackHandler(username, password));
//        try {
//            client = new MemcachedClient(
//                    new ConnectionFactoryBuilder().setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
//                            .setAuthDescriptor(ad)
//                            .build(),
//                    AddrUtil.getAddresses(proxy + ":" + port));
//        } catch (IOException e) {
//            logger.error("", e);
//        }
    }

    @BeforeMethod
    public void beforeMethod() {
        this.task = new OcsWriter.Task();
    }

    @AfterMethod
    public void afterMethod() {
        this.task = null;
    }

//    @Test
//    @Deprecated
//    public void testOcsWrite() throws InterruptedException {
//        String key1 = "key_xl";
//        String value1 = "value_xl";
//        String key2 = "key_zd";
//        String value2 = "value_zd";
//
//        client.set(key1, 200, value1);
//        client.set(key2, 200, value2);
//        logger.info(client.get(key1).toString());
//        logger.info(client.get(key2).toString());
//        Assert.assertEquals(client.get("key_zd"), "value_zd");
//        Object value_cl = client.get("key_cl");
//        Assert.assertEquals(value_cl, null);
//        logger.info(value_cl == null ? "value of key_cl is null" : value_cl.toString());
//        client.set(key1, 2, value1);
//        CommonUtils.sleepInMs(3000L);
//        Object value_new = client.get(key1);
//        logger.info(value_new == null ? String.format("value of %s is null", key1) : value_new.toString());
//        Assert.assertNull(client.get(key1));
//    }

    @Test
    public void testBuildKey_0() {
        Record record = EasyMock.createMock(Record.class);
        EasyMock.expect(record.getColumnNumber()).andReturn(4).anyTimes();
        EasyMock.expect(record.getColumn(1)).andReturn(null).anyTimes();
        EasyMock.replay(record);
        HashSet<Integer> index = new HashSet<Integer>();
        index.add(1);
        this.task.setIndexes(index);
        String act = this.task.buildKey_test(record);
        logger.info(act);
        Assert.assertEquals(act, "");
    }
    @Test
    public void testBuildKey_1() {
        ArrayList<Column> columns = new ArrayList<Column>();
        columns.add(new StringColumn("key_000"));
        Record record = EasyMock.createMock(Record.class);
        EasyMock.expect(record.getColumnNumber()).andReturn(4).anyTimes();
        EasyMock.expect(record.getColumn(0)).andReturn(columns.get(0)).anyTimes();
        EasyMock.replay(record);
        HashSet<Integer> index = new HashSet<Integer>();
        index.add(0);
        this.task.setIndexes(index);
        String act = this.task.buildKey_test(record);
        logger.info(act);
        Assert.assertEquals(act, "key_000");
    }

    @Test
    public void testBuildKey_2() {
        ArrayList<Column> columns = new ArrayList<Column>();
        columns.add(new StringColumn("key_000"));
        columns.add(new StringColumn("key_001"));
        Record record = EasyMock.createMock(Record.class);
        EasyMock.expect(record.getColumnNumber()).andReturn(4).anyTimes();
        EasyMock.expect(record.getColumn(0)).andReturn(columns.get(0)).anyTimes();
        EasyMock.expect(record.getColumn(1)).andReturn(columns.get(1)).anyTimes();
        EasyMock.replay(record);
        HashSet<Integer> index = new HashSet<Integer>();
        index.add(1);
        index.add(0);
        logger.info(index.toString());
        this.task.setIndexes(index);
        String act = this.task.buildKey_test(record);
        logger.info(act);
        Assert.assertEquals(act, "key_000\u0001key_001");
    }


    @Test
    public void testBuildValue_0() {
        ArrayList<Column> columns = new ArrayList<Column>();
        columns.add(new LongColumn(12345567890L));
        columns.add(new DoubleColumn(1.2345000000012));
        columns.add(new StringColumn("value_000"));
        columns.add(new DateColumn(new Date(1431595942234L)));
        columns.add(new BytesColumn("shit and shit".getBytes()));
        Record record = EasyMock.createMock(Record.class);
        EasyMock.expect(record.getColumnNumber()).andReturn(5).anyTimes();
        EasyMock.expect(record.getColumn(0)).andReturn(columns.get(0)).anyTimes();
        EasyMock.expect(record.getColumn(1)).andReturn(columns.get(1)).anyTimes();
        EasyMock.expect(record.getColumn(2)).andReturn(columns.get(2)).anyTimes();
        EasyMock.expect(record.getColumn(3)).andReturn(columns.get(3)).anyTimes();
        EasyMock.expect(record.getColumn(4)).andReturn(columns.get(4)).anyTimes();
        EasyMock.replay(record);
        String act = this.task.buildValue_test(record);
        logger.info(act);
        Assert.assertEquals(act, "12345567890\u00011.2345000000012\u0001value_000\u00012015-05-14 17:32:22.234\u0001c2hpdCBhbmQgc2hpdA==");
    }
    @Test
    public void testBuildValue_1() {
        ArrayList<Column> columns = new ArrayList<Column>();
        columns.add(new LongColumn(12345567890L));
        columns.add(new DoubleColumn(1.2345000000012));
        columns.add(new StringColumn("value_000"));
        columns.add(new DateColumn(new Date(1431595942234L)));
        columns.add(new BoolColumn(true));
        Record record = EasyMock.createMock(Record.class);
        EasyMock.expect(record.getColumnNumber()).andReturn(5).anyTimes();
        EasyMock.expect(record.getColumn(0)).andReturn(columns.get(0)).anyTimes();
        EasyMock.expect(record.getColumn(1)).andReturn(columns.get(1)).anyTimes();
        EasyMock.expect(record.getColumn(2)).andReturn(columns.get(2)).anyTimes();
        EasyMock.expect(record.getColumn(3)).andReturn(columns.get(3)).anyTimes();
        EasyMock.expect(record.getColumn(4)).andReturn(columns.get(4)).anyTimes();
        EasyMock.replay(record);
        String act = this.task.buildValue_test(record);
        logger.info(act);
        Assert.assertEquals(act, "12345567890\u00011.2345000000012\u0001value_000\u00012015-05-14 17:32:22.234\u0001true");
    }

    @Test
    public void testBuildValue_2() {
        ArrayList<Column> columns = new ArrayList<Column>();
        columns.add(new LongColumn(12345567890L));
        columns.add(new DoubleColumn(1.2345000000012));
        columns.add(new StringColumn("value_000"));
        columns.add(new DateColumn(new Date(1431595942234L)));
        columns.add(new BoolColumn(true));
        columns.add(null);
        Record record = EasyMock.createMock(Record.class);
        EasyMock.expect(record.getColumnNumber()).andReturn(5).anyTimes();
        EasyMock.expect(record.getColumn(0)).andReturn(columns.get(0)).anyTimes();
        EasyMock.expect(record.getColumn(1)).andReturn(columns.get(1)).anyTimes();
        EasyMock.expect(record.getColumn(2)).andReturn(columns.get(2)).anyTimes();
        EasyMock.expect(record.getColumn(3)).andReturn(columns.get(3)).anyTimes();
        EasyMock.expect(record.getColumn(4)).andReturn(columns.get(5)).anyTimes();
        EasyMock.replay(record);
        String act = this.task.buildValue_test(record);
        logger.info(act);
        Assert.assertEquals(act, "12345567890\u00011.2345000000012\u0001value_000\u00012015-05-14 17:32:22.234\u0001");
    }

    @AfterClass
    public void destroy() {
        if (task != null) {
            task.destroy();
        }
        if (client != null) {
            client.shutdown();
        }
    }
}
