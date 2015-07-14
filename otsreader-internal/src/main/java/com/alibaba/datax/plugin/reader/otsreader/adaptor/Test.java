package com.alibaba.datax.plugin.reader.otsreader.adaptor;

import java.io.UnsupportedEncodingException;

import org.apache.commons.codec.binary.Base64;

public class Test {
    
    public static void main(String[] args) throws UnsupportedEncodingException {
        // TODO Auto-generated method stub
        byte[] bytes = "hello".getBytes("UTF-8");
        String inutValue = Base64.encodeBase64String(bytes);
        System.out.println(inutValue);
    }
    
}
