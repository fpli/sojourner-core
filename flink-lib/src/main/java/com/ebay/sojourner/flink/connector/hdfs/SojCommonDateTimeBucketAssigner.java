package com.ebay.sojourner.flink.connector.hdfs;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.util.Preconditions;

public class SojCommonDateTimeBucketAssigner<T> implements BucketAssigner<T, String> {

  private static final long serialVersionUID = 1L;

  private static final String DEFAULT_FORMAT_STRING = "yyyyMMdd/HH";

  private final String formatString;

  private final ZoneId zoneId;

  private transient DateTimeFormatter dateTimeFormatter;

  public SojCommonDateTimeBucketAssigner() {
    this(DEFAULT_FORMAT_STRING);
  }

  public SojCommonDateTimeBucketAssigner(String formatString) {
    this(formatString, ZoneId.of("-7"));
  }

  public SojCommonDateTimeBucketAssigner(String formatString, ZoneId zoneId) {
    this.formatString = Preconditions.checkNotNull(formatString);
    this.zoneId = Preconditions.checkNotNull(zoneId);
  }

  @Override
  public String getBucketId(T element, BucketAssigner.Context context) {

    String defaultTsStr;

    if (dateTimeFormatter == null) {
      dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
    }

    defaultTsStr = dateTimeFormatter.format(Instant.ofEpochMilli(context.timestamp()));

    StringBuilder customTsBuilder = new StringBuilder();
    customTsBuilder
        .append("dt=").append(defaultTsStr.substring(0, 8))
        .append("/hr=").append(defaultTsStr.substring(9));
    return customTsBuilder.toString();

  }

  @Override
  public SimpleVersionedSerializer<String> getSerializer() {
    return SimpleVersionedStringSerializer.INSTANCE;
  }

  @Override
  public String toString() {
    return "SojCommonDateTimeBucketAssigner{"
        + "formatString='"
        + formatString
        + '\''
        + ", zoneId="
        + zoneId
        + '}';
  }
}
