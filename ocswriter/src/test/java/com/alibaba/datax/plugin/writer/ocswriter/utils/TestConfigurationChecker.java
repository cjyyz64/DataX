package com.alibaba.datax.plugin.writer.ocswriter.utils;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.ocswriter.Key;
import org.easymock.EasyMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Time:    2015-05-19 15:55
 * Creator: yuanqi@alibaba-inc.com
 */
@Test
public class TestConfigurationChecker {
    Logger logger = LoggerFactory.getLogger(TestConfigurationChecker.class);

    Configuration conf;

    @BeforeMethod
    public void setup() {
        conf = EasyMock.createMock(Configuration.class);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "proxy of ocs could not be blank")
    public void testProxy_0() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn(null).anyTimes();
        EasyMock.replay(conf);
        logger.info(conf.getString(Key.PROXY));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "proxy of ocs could not be blank")
    public void testProxy_1() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("").anyTimes();
        EasyMock.replay(conf);
        logger.info(conf.getString(Key.PROXY));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "port of ocs could not be blank")
    public void testPort_0() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn(null).anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn(null).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "user name could not be blank")
    public void testUser_0() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn(null).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "password could not be blank")
    public void testPassword_0() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.PASSWORD)).andReturn(null).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{},password:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER), conf.getString(Key.PASSWORD));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "indexes could not be blank")
    public void testIndex_0() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.PASSWORD)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES, "0")).andReturn(null).anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES)).andReturn(null).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{},password:{},index:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER), conf.getString(Key.PASSWORD), conf.getString(Key.INDEXES));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "illegal index")
    public void testIndex_1() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.PASSWORD)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES, "0")).andReturn("0,a").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES)).andReturn(null).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{},password:{},index:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER), conf.getString(Key.PASSWORD), conf.getString(Key.INDEXES));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "not supported write mode:null, recommended:set,add,replace,append,prepend")
    public void testWriteMode_0() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.PASSWORD)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES, "0")).andReturn("0,2").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES)).andReturn("2,3").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE)).andReturn(null).anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE, "set")).andReturn(null).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{},password:{},index:{},writeMode:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER), conf.getString(Key.PASSWORD), conf.getString(Key.INDEXES), conf.getString(Key.WRITE_MODE));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "not supported write mode:shit, recommended:set,add,replace,append,prepend")
    public void testWriteMode_1() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.PASSWORD)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES, "0")).andReturn("0,2").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES)).andReturn("2,3").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE)).andReturn(null).anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE, "set")).andReturn("shit").anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{},password:{},index:{},writeMode:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER), conf.getString(Key.PASSWORD), conf.getString(Key.INDEXES), conf.getString(Key.WRITE_MODE));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "not supported write format:null, recommended:text")
    public void testWriteFormat_0() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.PASSWORD)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES, "0")).andReturn("0,2").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES)).andReturn("2,3").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE)).andReturn("set").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE, "set")).andReturn("set").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_FORMAT, "text")).andReturn(null).anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_FORMAT)).andReturn(null).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{},password:{},index:{},writeMode:{},writeFormat:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER), conf.getString(Key.PASSWORD), conf.getString(Key.INDEXES), conf.getString(Key.WRITE_MODE), conf.getString(Key.WRITE_FORMAT));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "not supported write format:binary, recommended:text")
    public void testWriteFormat_1() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.PASSWORD)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES, "0")).andReturn("0,2").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES)).andReturn("2,3").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE)).andReturn("set").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE, "set")).andReturn("set").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_FORMAT, "text")).andReturn("binary").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_FORMAT)).andReturn("binary").anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{},password:{},index:{},writeMode:{},writeFormat:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER), conf.getString(Key.PASSWORD), conf.getString(Key.INDEXES), conf.getString(Key.WRITE_MODE), conf.getString(Key.WRITE_FORMAT));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "expire time must be bigger than 0")
    public void testExpireTime_0() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.PASSWORD)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES, "0")).andReturn("0,2").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES)).andReturn("2,3").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE)).andReturn("set").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE, "set")).andReturn("set").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_FORMAT, "text")).andReturn("text").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_FORMAT)).andReturn("text").anyTimes();
        EasyMock.expect(conf.getInt(Key.EXPIRE_TIME)).andReturn(0).anyTimes();
        EasyMock.expect(conf.getInt(Key.EXPIRE_TIME, Integer.MAX_VALUE)).andReturn(0).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{},password:{},index:{},writeMode:{},writeFormat:{},expireTime:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER), conf.getString(Key.PASSWORD), conf.getString(Key.INDEXES), conf.getString(Key.WRITE_MODE), conf.getString(Key.WRITE_FORMAT), conf.getInt(Key.EXPIRE_TIME));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "batch size must be bigger than 0")
    public void testBatchSize_0() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.PASSWORD)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES, "0")).andReturn("0,2").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES)).andReturn("2,3").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE)).andReturn("set").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE, "set")).andReturn("set").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_FORMAT, "text")).andReturn("text").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_FORMAT)).andReturn("text").anyTimes();
        EasyMock.expect(conf.getInt(Key.EXPIRE_TIME)).andReturn(1000).anyTimes();
        EasyMock.expect(conf.getInt(Key.EXPIRE_TIME, Integer.MAX_VALUE)).andReturn(100).anyTimes();
        EasyMock.expect(conf.getInt(Key.BATCH_SIZE)).andReturn(-1).anyTimes();
        EasyMock.expect(conf.getInt(Key.BATCH_SIZE, 100)).andReturn(-1).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{},password:{},index:{},writeMode:{},writeFormat:{},expireTime:{},batchSize:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER), conf.getString(Key.PASSWORD), conf.getString(Key.INDEXES), conf.getString(Key.WRITE_MODE), conf.getString(Key.WRITE_FORMAT), conf.getInt(Key.EXPIRE_TIME), conf.getInt(Key.BATCH_SIZE));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test
    public void testBatchSize_1() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.PASSWORD)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES, "0")).andReturn("0,2").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES)).andReturn("2,3").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE)).andReturn("set").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE, "set")).andReturn("set").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_FORMAT, "text")).andReturn("text").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_FORMAT)).andReturn("text").anyTimes();
        EasyMock.expect(conf.getInt(Key.EXPIRE_TIME)).andReturn(1000).anyTimes();
        EasyMock.expect(conf.getInt(Key.EXPIRE_TIME, Integer.MAX_VALUE)).andReturn(100).anyTimes();
        EasyMock.expect(conf.getInt(Key.BATCH_SIZE)).andReturn(1).anyTimes();
        EasyMock.expect(conf.getInt(Key.BATCH_SIZE, 100)).andReturn(1).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{},password:{},index:{},writeMode:{},writeFormat:{},expireTime:{},batchSize:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER), conf.getString(Key.PASSWORD), conf.getString(Key.INDEXES), conf.getString(Key.WRITE_MODE), conf.getString(Key.WRITE_FORMAT), conf.getInt(Key.EXPIRE_TIME), conf.getInt(Key.BATCH_SIZE));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "unknown host:abc")
    public void testHostReachableCheck_0() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("abc").anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{}", conf.getString(Key.PROXY));
        ConfigurationChecker.hostReachableCheck_test(conf);
    }

    @Test
    public void testHostReachableCheck_1() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.0.0.1").anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{}", conf.getString(Key.PROXY));
        ConfigurationChecker.hostReachableCheck_test(conf);
    }
}
