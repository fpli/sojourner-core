package com.ebay.sojourner.common.util;

import com.google.common.primitives.Longs;

public class ByteArrayUtils {
  public static byte[] fromBoolean(boolean value) {
    return new byte[]{(byte) (value ? 1 : 0)};
  }

  public static byte[] fromLong(long value) {
    return Longs.toByteArray(value);
  }

  public static boolean toBoolean(byte[] value) {
    if (value == null) return false;
    return value[0] == 1;
  }

  public static long toLong(byte[] value) {
    return Longs.fromByteArray(value);
  }
}
