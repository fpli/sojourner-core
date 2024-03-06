package com.ebay.sojourner.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RegexReplace {

  public RegexReplace() {
    super();
  }

  public static String replace(
      String field, String pattern, String replace, int start, int occurrence, char ignore) {
    if (field == null) {
      return null;
    }
    String result = null;
    int start_position = 0, end_position = 0;
    int count = 0;
    if (start < 0 || occurrence < 0) {
      log.info("fatal error: the position of regexp is wrong!! Return null.");
      return null;
    }

    Pattern p;
    // whether case sensitive
    if (ignore == 'i') {
      p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
    } else {
      p = Pattern.compile(pattern);
    }

    Matcher matcher = p.matcher(field);

    if (occurrence != 0) { // just replace the nth pattern
      while (matcher.find()) {
        count++;
        if (count == occurrence) {
          start_position = matcher.start();
          end_position = matcher.end();
          break;
        }
      }
      if (count < occurrence) {
        log.info("Can't find the " + occurrence + "th pattern! Return null.");
        return null;
      } else {
        // Kobe: use StringBuilder instead
        // result = field.substring(0, start_position) + replace + field.substring(end_position);
        StringBuilder valueBuilder = new StringBuilder(field.substring(0, start_position));
        valueBuilder.append(replace).append(field.substring(end_position));
        result = valueBuilder.toString();
      }
    } else {
      result = matcher.replaceAll(replace);
    }
    return result;
  }
}
