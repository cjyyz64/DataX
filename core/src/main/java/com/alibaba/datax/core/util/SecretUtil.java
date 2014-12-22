package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

/**
 * Created by jingxing on 14/12/15.
 */
public class SecretUtil {
    private static final String ENCODING = "UTF-8";

    private static final String KEY_ALGORITHM = "RSA";

    private static final Base64 base64 = new Base64();

    /**
     * BASE64加密
     *
     * @param plaintextBytes
     * @return
     * @throws Exception
     */
    public static String encryptBASE64(byte[] plaintextBytes) throws Exception {
        return new String(base64.encode(plaintextBytes), ENCODING);
    }

    /**
     * BASE64解密
     *
     * @param cipherText
     * @return
     * @throws Exception
     */
    public static byte[] decryptBASE64(String cipherText) {
        return base64.decode(cipherText);
    }

    /**
     * 加密<br>
     * 用公钥加密 encryptByPublicKey
     *
     * @param data 裸的原始数据
     * @param key 经过base64加密的公钥
     * @return 结果也采用base64加密
     * @throws Exception
     *
     */
    public static String encrypt(String data, String key) {
        try {
            // 对公钥解密，公钥被base64加密过
            byte[] keyBytes = decryptBASE64(key);

            // 取得公钥
            X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
            KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
            Key publicKey = keyFactory.generatePublic(x509KeySpec);

            // 对数据加密
            Cipher cipher = Cipher.getInstance(keyFactory.getAlgorithm());
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);

            return encryptBASE64(cipher.doFinal(data.getBytes(ENCODING)));
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR, "rsa加密出错", e);
        }
    }

    /**
     * 解密<br>
     * 用私钥解密
     *
     * @param data 已经经过base64加密的密文
     * @param key 已经经过base64加密私钥
     * @return
     * @throws Exception
     */
    public static String decrypt(String data, String key) {
        try {
            // 对密钥解密
            byte[] keyBytes = decryptBASE64(key);

            // 取得私钥
            PKCS8EncodedKeySpec pkcs8KeySpec = new PKCS8EncodedKeySpec(keyBytes);
            KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
            Key privateKey = keyFactory.generatePrivate(pkcs8KeySpec);

            // 对数据解密
            Cipher cipher = Cipher.getInstance(keyFactory.getAlgorithm());
            cipher.init(Cipher.DECRYPT_MODE, privateKey);

            return new String(cipher.doFinal(decryptBASE64(data)), ENCODING);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR, "rsa解密出错", e);
        }
    }

    /**
     * 初始化密钥
     *
     * @return
     * @throws Exception
     */
    public static String[] initKey() throws Exception {
        KeyPairGenerator keyPairGen = KeyPairGenerator
                .getInstance(KEY_ALGORITHM);
        keyPairGen.initialize(1024);

        KeyPair keyPair = keyPairGen.generateKeyPair();

        // 公钥
        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();

        // 私钥
        RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();

        String[] publicAndPrivateKey = {
                encryptBASE64(publicKey.getEncoded()),
                encryptBASE64(privateKey.getEncoded())};

        return publicAndPrivateKey;
    }
}
