package com.ebay.sojourner.common.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

// TODO: add UT for me
public class SojDateTimeUtils {

  public static ZoneId ebayServerZoneId() {
    return ZoneId.of("GMT-7");
  }

  public static long toSojTs(long epochMilli) {
    return (epochMilli * Constants.MILLI2MICRO) + Constants.OFFSET;
  }

  public static long toEpochMilli(long sojTs) {
    return (sojTs - Constants.OFFSET) / Constants.MILLI2MICRO;
  }

  public static String toDateString(long epochMilli, ZoneId zoneId, DateTimeFormatter formatter) {
    if (formatter == null) {
      // default format
      formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    }

    final String DEFAULT_DATE = "19700101";

    if (epochMilli > 0) {
      Instant instant = Instant.ofEpochMilli(epochMilli);
      LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zoneId);
      return dateTime.format(formatter);
    } else {
      return DEFAULT_DATE;
    }
  }

  public static String toDateString(long epochMilli) {
    return toDateString(epochMilli, ebayServerZoneId(), null);
  }

  public static String toDateString(long epochMilli, DateTimeFormatter formatter) {
    return toDateString(epochMilli, ebayServerZoneId(), formatter);
  }

  public static String toHrString(long epochMilli) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH");
    return toDateString(epochMilli, ebayServerZoneId(), formatter);
  }

}
