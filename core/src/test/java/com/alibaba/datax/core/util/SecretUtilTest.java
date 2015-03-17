package com.alibaba.datax.core.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by jingxing on 14/12/15.
 */
public class SecretUtilTest {
    @Test
    public void test() throws Exception {
        String[] keys = SecretUtil.initKey();
        String publicKey = keys[0];
        String privateKey = keys[1];
        System.out.println("publicKey:" + publicKey);
        System.out.println("privateKey:" + privateKey);
        String data = "阿里巴巴DataX";

        System.out.println("【加密前】：" + data);

        // 加密
        String cipherText = SecretUtil.encrypt(data, publicKey);
        System.out.println("【加密后】：" + cipherText);

        // 解密
        String plainText = SecretUtil.decrypt(cipherText, privateKey);
        System.out.println("【解密后】：" + plainText);

        Assert.assertTrue(plainText.equals(data));
    }
}
