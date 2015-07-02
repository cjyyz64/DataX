package com.alibaba.datax.plugin.writer.ocswriter.utils;

import com.alibaba.datax.common.exception.DataXException;
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
public class ConfigurationCheckerTest {
    Logger logger = LoggerFactory.getLogger(ConfigurationCheckerTest.class);
    Configuration conf;

    @BeforeMethod
    public void setup() {
        conf = EasyMock.createMock(Configuration.class);
    }

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "REQUIRED_VALUE - ocs服务地址proxy不能设置为空")
    public void testProxy_0() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn(null).anyTimes();
        EasyMock.replay(conf);
        logger.info(conf.getString(Key.PROXY));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "REQUIRED_VALUE - ocs服务地址proxy不能设置为空")
    public void testProxy_1() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("").anyTimes();
        EasyMock.replay(conf);
        logger.info(conf.getString(Key.PROXY));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "REQUIRED_VALUE - ocs端口port不能设置为空")
    public void testPort_0() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn("user").anyTimes();
        EasyMock.expect(conf.getString(Key.PASSWORD)).andReturn("user").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn(null).anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn(null).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "REQUIRED_VALUE - 访问ocs的用户userName不能设置为空")
    public void testUser_0() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn(null).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "REQUIRED_VALUE - 访问ocs的用户passWord不能设置为空")
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

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "REQUIRED_VALUE - 当做key的列编号indexes不能为空")
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

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "ILLEGAL_PARAM_VALUE - 列编号indexes必须为逗号分隔的非负整数")
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

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "REQUIRED_VALUE - 操作方式writeMode不能为空")
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

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "ILLEGAL_PARAM_VALUE - 不支持操作方式shit，仅支持set,add,replace,append,prepend")
    public void testWriteMode_1() {
        EasyMock.expect(conf.getString(Key.PROXY)).andReturn("127.1.1.1").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT, "11211")).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.PORT)).andReturn("110").anyTimes();
        EasyMock.expect(conf.getString(Key.USER)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.PASSWORD)).andReturn("zd").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES, "0")).andReturn("0,2").anyTimes();
        EasyMock.expect(conf.getString(Key.INDEXES)).andReturn("2,3").anyTimes();
        EasyMock.expect(conf.getString(Key.WRITE_MODE)).andReturn("shit").anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{},password:{},index:{},writeMode:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER), conf.getString(Key.PASSWORD), conf.getString(Key.INDEXES), conf.getString(Key.WRITE_MODE));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "REQUIRED_VALUE - 写入格式writeFormat不能为空")
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

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "ILLEGAL_PARAM_VALUE - 不支持写入格式binary，仅支持text")
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

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "ILLEGAL_PARAM_VALUE - 数据过期时间设置expireTime不能小于0")
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
        EasyMock.expect(conf.getInt(Key.EXPIRE_TIME)).andReturn(-1).anyTimes();
        EasyMock.expect(conf.getInt(Key.EXPIRE_TIME, 0)).andReturn(-1).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{},password:{},index:{},writeMode:{},writeFormat:{},expireTime:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER), conf.getString(Key.PASSWORD), conf.getString(Key.INDEXES), conf.getString(Key.WRITE_MODE), conf.getString(Key.WRITE_FORMAT), conf.getInt(Key.EXPIRE_TIME));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "ILLEGAL_PARAM_VALUE - 批量写入大小设置batchSize必须大于0")
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
        EasyMock.expect(conf.getInt(Key.EXPIRE_TIME, 0)).andReturn(100).anyTimes();
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
        EasyMock.expect(conf.getInt(Key.EXPIRE_TIME, 0)).andReturn(100).anyTimes();
        EasyMock.expect(conf.getInt(Key.BATCH_SIZE)).andReturn(1).anyTimes();
        EasyMock.expect(conf.getInt(Key.BATCH_SIZE, 100)).andReturn(1).anyTimes();
        EasyMock.replay(conf);
        logger.info("proxy:{},port:{},user:{},password:{},index:{},writeMode:{},writeFormat:{},expireTime:{},batchSize:{}", conf.getString(Key.PROXY), conf.getString(Key.PORT), conf.getString(Key.USER), conf.getString(Key.PASSWORD), conf.getString(Key.INDEXES), conf.getString(Key.WRITE_MODE), conf.getString(Key.WRITE_FORMAT), conf.getInt(Key.EXPIRE_TIME), conf.getInt(Key.BATCH_SIZE));
        ConfigurationChecker.paramCheck_test(conf);
    }

    @Test(expectedExceptions = DataXException.class, expectedExceptionsMessageRegExp = "HOST_UNREACHABLE - 不存在的host地址:abc")
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
