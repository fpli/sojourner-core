package com.ebay.sojourner.common.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


// FIXME: 1. remove joda, use java 8 time api instead.
//  2. consolidate all datetime utils into one class
public class SojTimestamp {

  private static DateTimeFormatter timestampFormatter =
      DateTimeFormat.forPattern(Constants.DEFAULT_TIMESTAMP_FORMAT)
          .withZone(
              DateTimeZone.forTimeZone(TimeZone.getTimeZone(Constants.EBAY_TIMEZONE)));
  private static DateTimeFormatter dateTimeFormatter =
      DateTimeFormat.forPattern(Constants.DEFAULT_DATE_FORMAT)
          .withZone(
              DateTimeZone.forTimeZone(TimeZone.getTimeZone(Constants.EBAY_TIMEZONE)));

  public static String getSojTimestamp(String s) {
    String res;
    //        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    //        sdf.setTimeZone(TimeZone.getTimeZone("GMT-7"));
    Date date = timestampFormatter.parseDateTime(s.substring(0, 23)).toDate();
    long ts = date.getTime();
    long sojTimestamp = (ts * Constants.MILLI2MICRO) + Constants.OFFSET;
    res = String.valueOf(sojTimestamp);
    return res;
  }

  public static String getDateToSojTimestamp(String s) {
    String res;
    //        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    //        sdf.setTimeZone(TimeZone.getTimeZone("GMT-7"));
    Date date = dateTimeFormatter.parseDateTime(s.substring(0, 10)).toDate();
    long ts = date.getTime();
    long sojTimestamp = (ts * Constants.MILLI2MICRO) + Constants.OFFSET;
    res = String.valueOf(sojTimestamp);
    return res;
  }

  public static Long getUnixTimestamp(String s) {
    Date date = timestampFormatter.parseDateTime(s.substring(0, 23)).toDate();
    long ts = date.getTime();
    return ts;
  }

  public static Long getSojTimestampToUnixTimestamp(Long s) {
    long ts = (s - Constants.OFFSET) / Constants.MILLI2MICRO;
    return ts;
  }

  /**
   * Get Sojourner default Calendar for being used.
   */
  public static Calendar getCalender() {
    return Calendar.getInstance(TimeZone.getTimeZone(Constants.EBAY_TIMEZONE), Locale.US);
  }

  public static DateFormat getDateFormat(String pattern) {
    DateFormat dateFormat = new SimpleDateFormat(pattern);
    // Make consistent with getCalender()
    dateFormat.setTimeZone(TimeZone.getTimeZone(Constants.EBAY_TIMEZONE));
    return dateFormat;
  }

  public static long getSojTimestamp(long milliseconds) {
    return (milliseconds * Constants.MILLI2MICRO) + Constants.OFFSET;
  }

  public static long getUnixTimestamp(long microseconds) {
    return (microseconds - Constants.OFFSET) / Constants.MILLI2MICRO;
  }

  public static long castSojTimestampToDate(long microseconds) {
    return microseconds - (microseconds % Constants.MICROECOFDAY);
  }

  public static long castUnixTimestampToDateMINS1(long millSeconds) {
    return millSeconds - (millSeconds % Constants.MILSECOFDAY)+Constants.MILSECOFDAYMINUS1;
  }

  public static long getUnixDate(long microseconds) {
    microseconds = castSojTimestampToDate(microseconds);
    return getUnixTimestamp(microseconds);
  }

  public static String getDateStrWithMillis(long ts) {
    try {
      return timestampFormatter
          .print((ts - Constants.OFFSET) / Constants.MILLI2MICRO);
    } catch (Exception e) {
      return null;
    }
  }

  public static String getDateStr(long ts) {
    try {
      return dateTimeFormatter.print((ts - Constants.OFFSET) / Constants.MILLI2MICRO);
    } catch (Exception e) {
      return null;
    }
  }

  public static Date getDate(long ts) {
    try {
      return new Date((ts - Constants.OFFSET) / Constants.MILLI2MICRO);
    } catch (Exception e) {
      return null;
    }
  }

  public static String getDateStrWithUnixTimestamp(long ts) {
    try {
      return dateTimeFormatter.print(ts);
    } catch (Exception e) {
      return null;
    }
  }

  public static String normalized(String ts) {
    try {
      long timestamp = Long.valueOf(ts.trim());
      timestamp = (timestamp - Constants.OFFSET) / Constants.MILLI2MICRO;
      return String.valueOf(timestamp);
    } catch (Exception e) {
      throw new RuntimeException("normalized timestamp failed", e);
    }
  }

  public static void main(String[] args) {
    System.out
        .println(
            getSojTimestampToUnixTimestamp(Long.valueOf(
                getSojTimestamp("2020-08-22 23:59:11.865"))));
    System.out.println(getUnixTimestamp("2020-06-17 02:59:59.000"));
    System.out.println(getSojTimestampToUnixTimestamp(3801622085446000L));
    //    System.out.println(getUnixTimestamp("2020-06-17 02:59:59.000"));
    System.out.println(getSojTimestampToUnixTimestamp(3807074683982000L));//1598111083982

    System.out.println(getSojTimestampToUnixTimestamp(3807076484397000L));//
    System.out.println(getDateStr(3807076484397000L));
    System.out.println(getDateStrWithMillis(3807076484397000L));
    System.out.println(getDateStrWithUnixTimestamp(1598111083982L));
  }
}
