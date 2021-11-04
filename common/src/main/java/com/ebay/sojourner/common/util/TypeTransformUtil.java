package com.ebay.sojourner.common.util;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class TypeTransformUtil {

  private static final Map<Character, Integer> dict = new HashMap<Character, Integer>();
  private static final Map<Integer, Character> reDict = new HashMap<Integer, Character>();
  private static final int LEN = 16;
  private static final int HOLE_LEN = 32;
  private static final int BIT_OFF = 4;
  private static final int OFFSET = 10;
  private static final long MARK = 0xf;

  static {
    for (int i = 0; i < 10; ++i) {
      dict.put((char) ('0' + i), i);
      reDict.put(i, (char) ('0' + i));
    }
    for (int i = 0; i < 6; ++i) {
      dict.put((char) ('a' + i), OFFSET + i);
      reDict.put(OFFSET + i, (char) ('a' + i));
    }
  }


  public static long[] md522Long(CharSequence cmd) {
    if (cmd == null) {
      return new long[]{Long.MIN_VALUE, Long.MIN_VALUE};
    }
    if (StringUtils.isBlank(cmd)) {
      return new long[]{0L, 0L};
    }
    String md = cmd.toString();
    int firstLen = LEN;
    int secondLen = 0;
    if (md.length() <= LEN) {
      firstLen = md.length();
    } else {
      secondLen = md.length();
    }
    long part1 = getOneLong(md.substring(0, firstLen));
    long part2 = 0;
    if (secondLen > 0) {
      part2 = getOneLong(md.substring(LEN, secondLen));
    }
    return new long[]{part1, part2};
  }

  private static Long getOneLong(String part) {
    long res = 0;
    for (int i = 0; i < part.length(); ++i) {
      char c = part.charAt(i);
      Integer bitC = dict.get(c);
      if (bitC == null) {
        // change behavior, convert illegal char. instead of return the whole as null
        bitC = Math.abs(c % 16);
      }
      long bb = bitC;
      res |= (bb << (i * BIT_OFF));
    }
    return res;
  }

  private static String recoveryPartMD5(long part) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < LEN; ++i) {
      sb.append(reDict.get((int) (part & MARK)));
      part >>= BIT_OFF;
    }
    return sb.toString();
  }

  public static CharSequence recoveryMD5(long part1, long part2) {
    return recoveryPartMD5(part1) + recoveryPartMD5(part2);
  }

  public static Integer long2Int(Long l) {
    if (l == null) {
      return null;
    }
    return l.intValue();
  }

  public static Integer ipToInt(CharSequence cipAddr) {
    if (StringUtils.isBlank(cipAddr)) {
      return null;
    }
    String ipAddr = cipAddr.toString();
    if (!IsValidIPv4.isValidIP(ipAddr)) {
      log.warn("Error format of IP: {}", ipAddr);
      return ipAddr.hashCode();
    }

    try {
      byte[] bytes = InetAddress.getByName(ipAddr).getAddress();
      if (bytes != null && bytes.length == 4) {
        return bytesToInt(bytes);
      } else {
        return ipAddr.hashCode();
      }
    } catch (Exception e) {
      return ipAddr.hashCode();
    }
  }

  public static String int2Ip(int ip) {
    StringBuilder sb = new StringBuilder();
    int MASK = 0xFF;
    sb.append((ip >> 24) & MASK).append(".");
    sb.append((ip >> 16) & MASK).append(".");
    sb.append((ip >> 8) & MASK).append(".");
    sb.append(ip & MASK);
    return sb.toString();
  }

  public static String getMD5(CharSequence cmessage) {
    if (cmessage == null) {
      return null;
    }
    String message = cmessage.toString();
    String md5str = "";
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");//NOSONAR
      byte[] input = message.getBytes("UTF-8");
      byte[] buff = md.digest(input);
      md5str = bytesToHex(buff);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
    return md5str;
  }

  public static String bytesToHex(byte[] bytes) {
    StringBuffer md5str = new StringBuffer();

    for (int i = 0; i < bytes.length; i++) {
      int digital = bytes[i];

      if (digital < 0) {
        digital += 256;
      }
      if (digital < 16) {
        md5str.append("0");
      }
      md5str.append(Integer.toHexString(digital));
    }
    return md5str.toString().toLowerCase();
  }

  private static int bytesToInt(byte[] bytes) {
    int addr = bytes[3] & 0xFF;
    addr |= bytes[2] << 8 & 0xFF00;
    addr |= bytes[1] << 16 & 0xFF0000;
    addr |= bytes[0] << 24 & 0xFF000000;
    return addr;
  }

  public static long toLongCheckNull(Long l) {
    if (l == null) {
      return 0;
    } else {
      return l;
    }
  }

  public static int toIntCheckNull(Integer l) {
    if (l == null) {
      return 0;
    } else {
      return l;
    }
  }

  public static String toStringCheckNull(CharSequence l) {
    if (l == null) {
      return null;
    } else {
      return l.toString();
    }
  }

  public static String toStringNotNull(CharSequence l) {
    if (l == null) {
      return "";
    } else {
      return l.toString();
    }
  }

  public static Integer getInteger(Object o) {
    if (StringUtils.isEmpty(getString(o))) {
      return null;
    } else {
      return Integer.valueOf(getString(o));
    }
  }

  public static Boolean getBoolean(Object o) {
    if (StringUtils.isEmpty(getString(o))) {
      return null;
    } else {
      return Boolean.valueOf(getString(o));
    }
  }


  public static Long getLong(Object o) {
    if (StringUtils.isEmpty(getString(o))) {
      return null;
    } else {
      return Long.valueOf(getString(o));
    }
  }

  public static String getString(Object o) {
    return (o != null && !"null".equals(o.toString())) ? o.toString() : null;
  }
}