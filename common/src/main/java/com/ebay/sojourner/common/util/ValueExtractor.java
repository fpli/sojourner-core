package com.ebay.sojourner.common.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class ValueExtractor {

  public static String extract(String value, String key) {
    String result = null;
    int beginIndex = 0, endIndex = 0;
    if (StringUtils.isBlank(value) || StringUtils.isBlank(key) || key.length() >= value.length()) {
      log.debug("Length of key is equal or larger than input string, Return null.");
      return result;
    }
    if ((beginIndex = value.indexOf("&" + key + "=")) >= 0) {
      endIndex = value.indexOf("&", beginIndex + key.length() + 2);
      if (endIndex == -1) {
        endIndex = value.length();
      }
      result = value.substring(beginIndex + key.length() + 2, endIndex);
    } else if ((beginIndex = value.indexOf("&!" + key + "=")) >= 0) {
      endIndex = value.indexOf("&", beginIndex + key.length() + 3);
      if (endIndex == -1) {
        endIndex = value.length();
      }
      result = value.substring(beginIndex + key.length() + 3, endIndex);
    } else if ((beginIndex = value.indexOf("&_" + key + "=")) >= 0) {
      endIndex = value.indexOf("&", beginIndex + key.length() + 3);
      if (endIndex == -1) {
        endIndex = value.length();
      }
      result = value.substring(beginIndex + key.length() + 3, endIndex);
    } else {
      log.debug("Tag to extract not find in input string, Return null.");
      result = null;
    }
    return result;
  }
}
