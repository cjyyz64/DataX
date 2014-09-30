/**
 *  (C) 2010-2014 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.datax.plugin.writer.odpswriter;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.security.SecureRandom;

/**
 * ����* DES�ӽ���,֧����delphi����(�ַ���������ͳһΪUTF-8)
 *
 * ����*
 *
 * ����* @author wym
 *
 * ����
 */

public class DESCipher {

	/**
	 * ����* ��Կ
	 *
	 * ����
	 */

	public static final String KEY = "u4Gqu4Z8";

	private final static String DES = "DES";

	/**
	 * ����* ����
	 *
	 * ����*
	 *
	 * ����* @param src
	 *
	 * ����* ����(�ֽ�)
	 *
	 * ����* @param key
	 *
	 * ����* ��Կ�����ȱ�����8�ı���
	 *
	 * ����* @return ����(�ֽ�)
	 *
	 * ����* @throws Exception
	 *
	 * ����
	 */

	public static byte[] encrypt(byte[] src, byte[] key) throws Exception {

		// DES�㷨Ҫ����һ�������ε������Դ

		SecureRandom sr = new SecureRandom();

		// ��ԭʼ�ܳ����ݴ���DESKeySpec����

		DESKeySpec dks = new DESKeySpec(key);

		// ����һ���ܳ׹�����Ȼ��������DESKeySpecת����

		// һ��SecretKey����

		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(DES);

		SecretKey securekey = keyFactory.generateSecret(dks);

		// Cipher����ʵ����ɼ��ܲ���

		Cipher cipher = Cipher.getInstance(DES);

		// ���ܳ׳�ʼ��Cipher����

		cipher.init(Cipher.ENCRYPT_MODE, securekey, sr);

		// ���ڣ���ȡ���ݲ�����

		// ��ʽִ�м��ܲ���

		return cipher.doFinal(src);

	}

	/**
	 * ����* ����
	 *
	 * ����*
	 *
	 * ����* @param src
	 *
	 * ����* ����(�ֽ�)
	 *
	 * ����* @param key
	 *
	 * ����* ��Կ�����ȱ�����8�ı���
	 *
	 * ����* @return ����(�ֽ�)
	 *
	 * ����* @throws Exception
	 *
	 * ����
	 */

	public static byte[] decrypt(byte[] src, byte[] key) throws Exception {

		// DES�㷨Ҫ����һ�������ε������Դ

		SecureRandom sr = new SecureRandom();

		// ��ԭʼ�ܳ����ݴ���һ��DESKeySpec����

		DESKeySpec dks = new DESKeySpec(key);

		// ����һ���ܳ׹�����Ȼ��������DESKeySpec����ת����

		// һ��SecretKey����

		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(DES);

		SecretKey securekey = keyFactory.generateSecret(dks);

		// Cipher����ʵ����ɽ��ܲ���

		Cipher cipher = Cipher.getInstance(DES);

		// ���ܳ׳�ʼ��Cipher����

		cipher.init(Cipher.DECRYPT_MODE, securekey, sr);

		// ���ڣ���ȡ���ݲ�����

		// ��ʽִ�н��ܲ���

		return cipher.doFinal(src);

	}

	/**
	 * ����* ����
	 *
	 * ����*
	 *
	 * ����* @param src
	 *
	 * ����* ����(�ֽ�)
	 *
	 * ����* @return ����(�ֽ�)
	 *
	 * ����* @throws Exception
	 *
	 * ����
	 */

	public static byte[] encrypt(byte[] src) throws Exception {

		return encrypt(src, KEY.getBytes());

	}

	/**
	 * ����* ����
	 *
	 * ����*
	 *
	 * ����* @param src
	 *
	 * ����* ����(�ֽ�)
	 *
	 * ����* @return ����(�ֽ�)
	 *
	 * ����* @throws Exception
	 *
	 * ����
	 */

	public static byte[] decrypt(byte[] src) throws Exception {

		return decrypt(src, KEY.getBytes());

	}

	/**
	 * ����* ����
	 *
	 * ����*
	 *
	 * ����* @param src
	 *
	 * ����* ����(�ַ���)
	 *
	 * ����* @return ����(16�����ַ���)
	 *
	 * ����* @throws Exception
	 *
	 * ����
	 */

	public final static String encrypt(String src) {

		try {

			return byte2hex(encrypt(src.getBytes(), KEY.getBytes()));

		} catch (Exception e) {

			e.printStackTrace();

		}

		return null;

	}

	/**
	 * ����* ����
	 *
	 * ����*
	 *
	 * ����* @param src
	 *
	 * ����* ����(�ַ���)
	 *
	 * ����* @return ����(�ַ���)
	 *
	 * ����* @throws Exception
	 *
	 * ����
	 */

	public final static String decrypt(String src) {
		try {

			return new String(decrypt(hex2byte(src.getBytes()), KEY.getBytes()));

		} catch (Exception e) {

			e.printStackTrace();

		}

		return null;

	}

	/**
	 * ����* ����
	 *
	 * ����*
	 *
	 * ����* @param src
	 *
	 * ����* ����(�ֽ�)
	 *
	 * ����* @return ����(16�����ַ���)
	 *
	 * ����* @throws Exception
	 *
	 * ����
	 */

	public static String encryptToString(byte[] src) throws Exception {

		return encrypt(new String(src));

	}

	/**
	 * ����* ����
	 *
	 * ����*
	 *
	 * ����* @param src
	 *
	 * ����* ����(�ֽ�)
	 *
	 * ����* @return ����(�ַ���)
	 *
	 * ����* @throws Exception
	 *
	 * ����
	 */

	public static String decryptToString(byte[] src) throws Exception {

		return decrypt(new String(src));

	}

	public static String byte2hex(byte[] b) {

		String hs = "";

		String stmp = "";

		for (int n = 0; n < b.length; n++) {

			stmp = (Integer.toHexString(b[n] & 0XFF));

			if (stmp.length() == 1)

				hs = hs + "0" + stmp;

			else

				hs = hs + stmp;

		}

		return hs.toUpperCase();

	}

	public static byte[] hex2byte(byte[] b) {

		if ((b.length % 2) != 0)

			throw new IllegalArgumentException("���Ȳ���ż��");

		byte[] b2 = new byte[b.length / 2];

		for (int n = 0; n < b.length; n += 2) {

			String item = new String(b, n, 2);

			b2[n / 2] = (byte) Integer.parseInt(item, 16);

		}
		return b2;

	}

	/*
	 * public static void main(String[] args) { try { String src = "cheetah";
	 * String crypto = DESCipher.encrypt(src); System.out.println("����[" + src +
	 * "]:" + crypto); System.out.println("���ܺ�:" + DESCipher.decrypt(crypto)); }
	 * catch (Exception e) { e.printStackTrace(); } }
	 */
}
