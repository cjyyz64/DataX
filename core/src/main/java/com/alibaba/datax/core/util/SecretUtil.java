package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.crypto.Cipher;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jingxing on 14/12/15.
 */
public class SecretUtil {
    private static Properties properties;

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
     * @param key  经过base64加密的公钥
     * @return 结果也采用base64加密
     * @throws Exception
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
     * @param key  已经经过base64加密私钥
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

    public static synchronized Properties getSecurityProperties() {
        if (properties == null) {
            InputStream secretStream = null;
            try {
                secretStream = new FileInputStream(
                        CoreConstant.DATAX_SECRET_PATH);
            } catch (FileNotFoundException e) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.SECRET_ERROR,
                        "DataX配置要求加解密，但无法找到密钥的配置文件");
            }

            properties = new Properties();
            try {
                properties.load(secretStream);
                secretStream.close();
            } catch (IOException e) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.SECRET_ERROR, "读取加解密配置文件出错", e);
            }
        }

        return properties;
    }


    public static Configuration encryptSecretKey(Configuration configuration) {
        String keyVersion = configuration
                .getString(CoreConstant.DATAX_JOB_SETTING_KEYVERSION);
        // 没有设置keyVersion，表示不用解密
        if (StringUtils.isBlank(keyVersion)) {
            return configuration;
        }

        Map<String,Pair<String,String>> versionKeyMap = getPrivateKeyMap();

        String publicKey = versionKeyMap.get(keyVersion).getRight();
        // keyVersion要求的私钥没有配置
        if (StringUtils.isBlank(publicKey)) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR,
                    String.format("DataX配置的密钥版本为[%s]，但在系统中没有配置，可能是任务密钥配置错误，也可能是系统维护问题", keyVersion));
        }

        String tempEncrptedData = null;
        for (String path : configuration.getSecretKeyPathSet()) {
            tempEncrptedData = SecretUtil.encrypt(configuration.getString(path), publicKey);

            int lastPathIndex = path.lastIndexOf(".") + 1;
            String lastPathKey = path.substring(lastPathIndex);

            String newPath = path.substring(0, lastPathIndex) + "*"
                    + lastPathKey;
            configuration.set(newPath, tempEncrptedData);
            configuration.remove(path);
        }

        return configuration;
    }

    public static Configuration decryptSecretKey(Configuration config) {
        String keyVersion = config
                .getString(CoreConstant.DATAX_JOB_SETTING_KEYVERSION);
        // 没有设置keyVersion，表示不用解密
        if (StringUtils.isBlank(keyVersion)) {
            return config;
        }

        Map<String,Pair<String,String>> versionKeyMap = getPrivateKeyMap();
        String privateKey = versionKeyMap.get(keyVersion).getLeft();
        // keyVersion要求的私钥没有配置
        if (StringUtils.isBlank(privateKey)) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR,
                    String.format("DataX配置的密钥版本为[%s]，但在系统中没有配置，可能是任务密钥配置错误，也可能是系统维护问题", keyVersion));
        }

        // 对包含*号key解密处理
        for (String key : config.getKeys()) {
            int lastPathIndex = key.lastIndexOf(".") + 1;
            String lastPathKey = key.substring(lastPathIndex);
            if (lastPathKey.length() > 1 && lastPathKey.charAt(0) == '*'
                    && lastPathKey.charAt(1) != '*') {
                Object value = config.get(key);
                if (value instanceof String) {
                    String newKey = key.substring(0, lastPathIndex)
                            + lastPathKey.substring(1);
                    config.set(newKey,
                            SecretUtil.decrypt((String) value, privateKey));
                    config.addSecretKeyPath(newKey);
                    config.remove(key);
                }
            }
        }

        return config;
    }

    private static Map<String,Pair<String,String>> getPrivateKeyMap() {
        // Key：keyVersion   value:left:privateKey, right:publicKey
        Map<String,Pair<String,String>> versionKeyMap = new HashMap<String, Pair<String, String>>();

        Properties properties = SecretUtil.getSecurityProperties();

        String lastKeyVersion = properties.getProperty(
                CoreConstant.LAST_KEYVERSION);
        String lastPublicKey = properties.getProperty(
                CoreConstant.LAST_PUBLICKEY);
        String lastPrivateKey = properties.getProperty(
                CoreConstant.LAST_PRIVATEKEY);
        if (StringUtils.isNotBlank(lastKeyVersion)) {
            if (StringUtils.isBlank(lastPublicKey) ||
                    StringUtils.isBlank(lastPrivateKey)) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.SECRET_ERROR,
                        "DataX配置要求加解密，但上次配置的公私钥对存在为空的情况"
                );
            }

            versionKeyMap.put(lastKeyVersion, ImmutablePair.of(lastPrivateKey,lastPublicKey));
        }

        String currentKeyVersion = properties.getProperty(
                CoreConstant.CURRENT_KEYVERSION);
        String currentPublicKey = properties.getProperty(
                CoreConstant.CURRENT_PUBLICKEY);
        String currentPrivateKey = properties.getProperty(
                CoreConstant.CURRENT_PRIVATEKEY);
        if (StringUtils.isNotBlank(currentKeyVersion)) {
            if (StringUtils.isBlank(currentPublicKey) ||
                    StringUtils.isBlank(currentPrivateKey)) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.SECRET_ERROR,
                        "DataX配置要求加解密，但当前配置的公私钥对存在为空的情况");
            }

            versionKeyMap.put(currentKeyVersion, ImmutablePair.of(currentPrivateKey,currentPublicKey));
        }

        if (versionKeyMap.size() <= 0) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR,
                    "DataX配置要求加解密，但无法找到公私钥");
        }

        return versionKeyMap;
    }
}
