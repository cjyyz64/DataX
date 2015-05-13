package com.alibaba.datax.plugin.writer.ocswriter;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

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

    @BeforeClass
    public void setup() {

        AuthDescriptor ad = new AuthDescriptor(new String[]{"PLAIN"}, new PlainCallbackHandler(username, password));
        try {
            client = new MemcachedClient(
                    new ConnectionFactoryBuilder().setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                            .setAuthDescriptor(ad)
                            .build(),
                    AddrUtil.getAddresses(proxy + ":" + port));
        } catch (IOException e) {
            logger.error("", e);
        }
    }

    @Test
    public void testOcsWrite() throws InterruptedException {
        String key1 = "key_xl";
        String value1 = "value_xl";
        String key2 = "key_zd";
        String value2 = "value_zd";

        client.set(key1, 200, value1);
        client.set(key2, 200, value2);
        logger.info(client.get(key1).toString());
        logger.info(client.get(key2).toString());
        Assert.assertEquals(client.get("key_zd"), "value_zd");
        Assert.assertEquals(client.get("key_cl"), null);
    }
    enum TEST {
        HAHA,
        shit
    }

    @Test
    public void tttest() {
        System.out.println(EnumUtils.isValidEnum(TEST.class, "haha"));
        System.out.println(EnumUtils.isValidEnum(TEST.class, "HAHA"));
        System.out.println(EnumUtils.isValidEnum(TEST.class, "SHIT"));
        System.out.println(EnumUtils.isValidEnum(TEST.class, "shit"));
        System.out.println(StringUtils.join(TEST.values(), ","));

        System.out.print("\nshit\u0001\u0001\n");
        System.out.println("piss");

    }

    @AfterClass
    public void destroy() {
        if (client != null) {
            client.shutdown();
        }
    }
}
