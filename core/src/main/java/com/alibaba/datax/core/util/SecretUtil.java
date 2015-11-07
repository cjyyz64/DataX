package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

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

    //RSA Key：keyVersion   value:left:privateKey, right:publicKey
    //DESede Key: keyVersion   value:left:keyContent, right:keyContent
    private static Map<String, Pair<String, String>> versionKeyMap;

    private static final String ENCODING = "UTF-8";

    private static final String KEY_ALGORITHM_RSA = "RSA";
    
    private static final String KEY_ALGORITHM_3DES = "DESede";
    
    private static final String CIPHER_ALGORITHM_3DES = "DESede/ECB/PKCS5Padding";
    
    private static String KEY_ALGORITHM = "RSA";

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
     * @param data 裸的原始数据
     * @param key  经过base64加密的公钥(RSA)或者裸密钥(3DES)
     * */
    public static String encrypt(String data, String key) {
        if (SecretUtil.KEY_ALGORITHM.equals(SecretUtil.KEY_ALGORITHM_RSA)) {
            return SecretUtil.encryptRSA(data, key);
        } else if (SecretUtil.KEY_ALGORITHM
                .equals(SecretUtil.KEY_ALGORITHM_3DES)) {
            return SecretUtil.encrypt3DES(data, key);
        } else {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR,
                    String.format("系统编程错误,不支持的加密类型", SecretUtil.KEY_ALGORITHM));
        }
    }
    
    /**
     * 解密<br>
     * @param data 已经经过base64加密的密文
     * @param key  已经经过base64加密私钥(RSA)或者裸密钥(3DES)
     * */
    public static String decrypt(String data, String key) {
        if (SecretUtil.KEY_ALGORITHM.equals(SecretUtil.KEY_ALGORITHM_RSA)) {
            return SecretUtil.decryptRSA(data, key);
        } else if (SecretUtil.KEY_ALGORITHM
                .equals(SecretUtil.KEY_ALGORITHM_3DES)) {
            return SecretUtil.decrypt3DES(data, key);
        } else {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR,
                    String.format("系统编程错误,不支持的加密类型", SecretUtil.KEY_ALGORITHM));
        }
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
    public static String encryptRSA(String data, String key) {
        try {
            // 对公钥解密，公钥被base64加密过
            byte[] keyBytes = decryptBASE64(key);

            // 取得公钥
            X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
            KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM_RSA);
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
    public static String decryptRSA(String data, String key) {
        try {
            // 对密钥解密
            byte[] keyBytes = decryptBASE64(key);

            // 取得私钥
            PKCS8EncodedKeySpec pkcs8KeySpec = new PKCS8EncodedKeySpec(keyBytes);
            KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM_RSA);
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
     * 初始化密钥 for RSA ALGORITHM
     *
     * @return
     * @throws Exception
     */
    public static String[] initKey() throws Exception {
        KeyPairGenerator keyPairGen = KeyPairGenerator
                .getInstance(KEY_ALGORITHM_RSA);
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
    
    /**
     * 加密 DESede<br>
     * 用密钥加密
     *
     * @param data 裸的原始数据
     * @param key  加密的密钥
     * @return 结果也采用base64加密
     * @throws Exception
     */
    public static String encrypt3DES(String data, String key) {
        try {
            // 生成密钥
            SecretKey desKey = new SecretKeySpec(build3DesKey(key),
                    KEY_ALGORITHM_3DES);
            // 对数据加密
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM_3DES);
            cipher.init(Cipher.ENCRYPT_MODE, desKey);
            return encryptBASE64(cipher.doFinal(data.getBytes(ENCODING)));
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR, "3重DES加密出错", e);
        }
    }
    
    /**
     * 解密<br>
     * 用密钥解密
     *
     * @param data 已经经过base64加密的密文
     * @param key  解密的密钥
     * @return
     * @throws Exception
     */
    public static String decrypt3DES(String data, String key) {
        try {
            // 生成密钥
            SecretKey desKey = new SecretKeySpec(build3DesKey(key),
                    KEY_ALGORITHM_3DES);
            // 对数据解密
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM_3DES);
            cipher.init(Cipher.DECRYPT_MODE, desKey);
            return new String(cipher.doFinal(decryptBASE64(data)), ENCODING);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR, "rsa解密出错", e);
        }
    }
    
    /**
     * 根据字符串生成密钥字节数组
     * 
     * @param keyStr
     *            密钥字符串
     * @return key 符合DESede标准的24byte数组
     */
    private static byte[] build3DesKey(String keyStr) {
        try {
            // 声明一个24位的字节数组，默认里面都是0，warn: 字符串0(48)和数组默认值0不一样，统一字符串0(48)
            byte[] key = "000000000000000000000000".getBytes(ENCODING);
            byte[] temp = keyStr.getBytes(ENCODING);
            if (key.length > temp.length) {
                // 如果temp不够24位，则拷贝temp数组整个长度的内容到key数组中
                System.arraycopy(temp, 0, key, 0, temp.length);
            } else {
                // 如果temp大于24位，则拷贝temp数组24个长度的内容到key数组中
                System.arraycopy(temp, 0, key, 0, key.length);
            }
            return key;
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR, "构建三重DES密匙出错", e);
        }
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

        Map<String, Pair<String, String>> versionKeyMap = getPrivateKeyMap();

        String key = versionKeyMap.get(keyVersion).getRight();
        // keyVersion要求的私钥没有配置
        if (StringUtils.isBlank(key)) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR,
                    String.format("DataX配置的密钥版本为[%s]，但在系统中没有配置，可能是任务密钥配置错误，也可能是系统维护问题", keyVersion));
        }

        String tempEncrptedData = null;
        for (String path : configuration.getSecretKeyPathSet()) {
            tempEncrptedData = SecretUtil.encrypt(configuration.getString(path), key);
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

        Map<String, Pair<String, String>> versionKeyMap = getPrivateKeyMap();
        String decryptKey = versionKeyMap.get(keyVersion).getLeft();
        // keyVersion要求的私钥没有配置
        if (StringUtils.isBlank(decryptKey)) {
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
                            SecretUtil.decrypt((String) value, decryptKey));
                    config.addSecretKeyPath(newKey);
                    config.remove(key);
                }
            }
        }

        return config;
    }

    private static synchronized Map<String, Pair<String, String>> getPrivateKeyMap() {
        if (versionKeyMap == null) {
            versionKeyMap = new HashMap<String, Pair<String, String>>();
            Properties properties = SecretUtil.getSecurityProperties();
            
            String lastKeyContent = properties
                    .getProperty(CoreConstant.LAST_KEYCONTENT);
            String currentKeyContent = properties
                    .getProperty(CoreConstant.CURRENT_KEYCONTENT);

            if (StringUtils.isNotBlank(lastKeyContent)
                    || StringUtils.isNotBlank(currentKeyContent)) {
                SecretUtil.KEY_ALGORITHM = SecretUtil.KEY_ALGORITHM_3DES;
                String lastKeyVersion = properties
                        .getProperty(CoreConstant.LAST_KEYVERSION);
                if (StringUtils.isNotBlank(lastKeyVersion)) {
                    if (StringUtils.isBlank(lastKeyContent)) {
                        throw DataXException.asDataXException(
                                FrameworkErrorCode.SECRET_ERROR,
                                "DataX配置要求加解密，但上次配置的密钥存在为空的情况");
                    }
                    versionKeyMap.put(lastKeyVersion, ImmutablePair.of(lastKeyContent, lastKeyContent));
                }
                String currentKeyVersion = properties
                        .getProperty(CoreConstant.CURRENT_KEYVERSION);
                if (StringUtils.isNotBlank(currentKeyVersion)) {
                    if (StringUtils.isBlank(currentKeyContent)) {
                        throw DataXException.asDataXException(
                                FrameworkErrorCode.SECRET_ERROR,
                                "DataX配置要求加解密，但当前配置的密钥存在为空的情况");
                    }
                    versionKeyMap.put(currentKeyVersion, ImmutablePair.of(currentKeyContent, lastKeyContent));
                }
            } else {
                SecretUtil.KEY_ALGORITHM = SecretUtil.KEY_ALGORITHM_RSA;
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

                    versionKeyMap.put(lastKeyVersion, ImmutablePair.of(lastPrivateKey, lastPublicKey));
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

                    versionKeyMap.put(currentKeyVersion, ImmutablePair.of(currentPrivateKey, currentPublicKey));
                }
            }
            
            if (versionKeyMap.size() <= 0) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.SECRET_ERROR,
                        "DataX配置要求加解密，但无法找到加解密配置");
            }
        }

        return versionKeyMap;
    }
}
