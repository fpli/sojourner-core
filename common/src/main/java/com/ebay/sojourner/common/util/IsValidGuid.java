package com.ebay.sojourner.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public class IsValidGuid {
  private static Pattern validGuidPattern = Pattern.compile("[01234567890abcdefABCDEF]+");

  public static boolean isValidGuid(String guid) {
    if(StringUtils.isBlank(guid)){
      return false;
    }
    Matcher matcher = validGuidPattern.matcher(guid);
    return matcher.matches();
  }
}
