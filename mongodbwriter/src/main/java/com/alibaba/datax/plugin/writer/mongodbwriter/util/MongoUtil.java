package com.alibaba.datax.plugin.writer.mongodbwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.mongodbwriter.KeyConstant;
import com.alibaba.datax.plugin.writer.mongodbwriter.MongoDBWriterErrorCode;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by jianying.wcj on 2015/3/17 0017.
 */
public class MongoUtil {

    public static MongoClient initMongoClient(Configuration conf) {

        String address = conf.getString(KeyConstant.MONGO_ADDRESS);
        if(Strings.isNullOrEmpty(address)) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        }
        try {
            return new MongoClient(parseServerAddress(address));
        } catch (UnknownHostException e) {
           throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
        } catch (NumberFormatException e) {
           throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
        }
    }

    public static MongoClient initCredentialMongoClient(Configuration conf,String userName,String password) {

        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
        for(Object obj : addressList) {
            System.out.println(obj);
        }
        String address = "";
        if(isHostPortPattern(address)) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        }
        try {
            MongoCredential credential = MongoCredential.createMongoCRCredential(userName, "admin", password.toCharArray());
            return new MongoClient(parseServerAddress(address), Arrays.asList(credential));

        } catch (UnknownHostException e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
        } catch (NumberFormatException e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
        }
    }
    /**
     * 判断地址类型是否符合要求
     * @param addressListStr
     * @return
     */
    private static boolean isHostPortPattern(String addressListStr) {
        Iterable<String> ms = Splitter.on(",").split(addressListStr);
        boolean isMatch = false;
        for(String address : ms) {
            String regex = "([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+):([0-9]+)";
            if(address.matches(regex)) {
                isMatch = true;
            }
        }
        return isMatch;
    }
    /**
     * 转换为mongo地址协议
     * @param address
     * @return
     */
    private static List<ServerAddress> parseServerAddress(String address) throws UnknownHostException{
        List<ServerAddress> addressList = new ArrayList<ServerAddress>();
        Map<String,String> ms = Splitter.on(",").withKeyValueSeparator(":").split(address);
        for(Map.Entry<String,String> temp : ms.entrySet()) {
            try {
                ServerAddress sa = new ServerAddress(temp.getKey(),Integer.valueOf(temp.getValue()));
                addressList.add(sa);
            } catch (UnknownHostException e) {
                throw new UnknownHostException();
            }
        }
        return addressList;
    }

    public static void main(String[] args) {
        try {
            List<ServerAddress> addressList = MongoUtil.parseServerAddress("127.0.0.1:27017");
            System.out.println(addressList.size());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
