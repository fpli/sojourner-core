package com.ebay.sojourner.common.util;

public class ByteArrayUtils {
  public static byte[] fromBoolean(boolean value) {
    return new byte[]{(byte) (value ? 1 : 0)};
  }

  public static boolean toBoolean(byte[] value) {
    return value[0] == 1;
  }

}
