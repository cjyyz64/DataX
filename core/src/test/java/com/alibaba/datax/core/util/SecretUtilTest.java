package com.alibaba.datax.core.util;

import java.lang.reflect.Field;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by jingxing on 14/12/15.
 */
public class SecretUtilTest {

    public void testRsa() throws Exception {
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

    public void testDes() throws Exception {
        String keyContent = "datax&cdp&dsc";
        System.out.println("keyContent:" + keyContent);
        String data = "阿里巴巴DataX";

        System.out.println("【加密前】：" + data);

        // 加密
        String cipherText = SecretUtil.encrypt(data, keyContent);
        System.out.println("【加密后】：" + cipherText);

        // 解密
        String plainText = SecretUtil.decrypt(cipherText, keyContent);
        System.out.println("【解密后】：" + plainText);

        Assert.assertTrue(plainText.equals(data));
    }

    @Test
    public void test() throws Exception {
        Field field = SecretUtil.class.getDeclaredField("KEY_ALGORITHM");
        field.setAccessible(true);
        field.set(SecretUtil.class, "RSA");
        this.testRsa();
        System.out.println("\n\n");
        field.set(SecretUtil.class, "DESede");
        this.testDes();

        try {
            field.set(SecretUtil.class, "RDS");
            this.testDes();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }
}
