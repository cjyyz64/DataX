package com.alibaba.datax.plugin.writer.hbasebulkwriter2.util;

import org.apache.hadoop.hbase.util.Bytes;

public class PhoenixEncoder {
  public static byte[] encodeInt(int v) {
    byte[] b = new byte[Bytes.SIZEOF_INT];
    int o = 0;
    b[o + 0] = (byte) ((v >> 24) ^ 0x80); // Flip sign bit so that INTEGER is
                                          // binary comparable
    b[o + 1] = (byte) (v >> 16);
    b[o + 2] = (byte) (v >> 8);
    b[o + 3] = (byte) v;
    return b;
  }

  public static byte[] encodeLong(long v) {
    byte[] b = new byte[Bytes.SIZEOF_LONG];
    int o = 0;
    b[o + 0] = (byte) ((v >> 56) ^ 0x80); // Flip sign bit so that INTEGER is
                                          // binary comparable
    b[o + 1] = (byte) (v >> 48);
    b[o + 2] = (byte) (v >> 40);
    b[o + 3] = (byte) (v >> 32);
    b[o + 4] = (byte) (v >> 24);
    b[o + 5] = (byte) (v >> 16);
    b[o + 6] = (byte) (v >> 8);
    b[o + 7] = (byte) v;
    return b;
  }

  public static byte[] encodeShort(short v) {
    byte[] b = new byte[Bytes.SIZEOF_SHORT];
    int o = 0;
    b[o + 0] = (byte) ((v >> 8) ^ 0x80); // Flip sign bit so that Short is
                                         // binary comparable
    b[o + 1] = (byte) v;
    return b;
  }

  public static byte[] encodeDouble(double v) {
    byte[] b = new byte[Bytes.SIZEOF_LONG];
    int o = 0;
    long l = Double.doubleToLongBits(v);
    l = (l ^ ((l >> Long.SIZE - 1) | Long.MIN_VALUE)) + 1;
    Bytes.putLong(b, o, l);
    return b;
  }

  public static byte[] encodeFloat(float v) {
    byte[] b = new byte[Bytes.SIZEOF_INT];
    int o = 0;
    int i = Float.floatToIntBits(v);
    i = (i ^ ((i >> Integer.SIZE - 1) | Integer.MIN_VALUE)) + 1;
    Bytes.putInt(b, o, i);
    return b;
  }

  // Compute the hash of the key value stored in key and set its first byte as
  // the value. The
  // first byte of key should be left empty as a place holder for the salting
  // byte.
  public static byte[] getSaltedKey(byte[] key, int bucketNum) {
    byte saltByte = getSaltingByte(key, bucketNum);
    return Bytes.add(new byte[] { saltByte }, key);
  }

  // Generate the bucket byte given a byte array and the number of buckets.
  public static byte getSaltingByte(byte[] value, int bucketNum) {
    int hash = hashCode(value);
    byte bucketByte = (byte) ((Math.abs(hash) % bucketNum));
    return bucketByte;
  }

  private static int hashCode(byte a[]) {
    if (a == null)
      return 0;
    int result = 1;
    for (int i = 0; i < a.length; i++) {
      result = 31 * result + a[i];
    }
    return result;
  }
}
