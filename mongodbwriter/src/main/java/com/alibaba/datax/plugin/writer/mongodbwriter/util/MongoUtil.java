package com.alibaba.datax.plugin.writer.mongodbwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.mongodbwriter.KeyConstant;
import com.alibaba.datax.plugin.writer.mongodbwriter.MongoDBWriterErrorCode;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;

/**
 * Created by jianying.wcj on 2015/3/17 0017.
 */
public class MongoUtil {

    public static MongoClient initMongoClient(Configuration conf) {

        String host = conf.getString(KeyConstant.MONGO_HOST);
        String port = conf.getString(KeyConstant.MONGO_PORT);

        if(Strings.isNullOrEmpty(host) || Strings.isNullOrEmpty(port)) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        }
        try {
            return new MongoClient(host,Integer.valueOf(port));
        } catch (UnknownHostException e) {
           throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
        } catch (NumberFormatException e) {
           throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
        }
    }
}
