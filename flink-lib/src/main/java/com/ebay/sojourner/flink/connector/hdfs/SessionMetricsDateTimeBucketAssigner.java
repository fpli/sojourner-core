package com.ebay.sojourner.flink.connector.hdfs;

import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.util.SojTimestamp;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.util.Preconditions;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Deprecated
public class SessionMetricsDateTimeBucketAssigner implements BucketAssigner<SessionMetrics, String> {

  private static final long serialVersionUID = 1L;

  private static final String DEFAULT_FORMAT_STRING = "yyyyMMdd/HH";

  private final String formatString;

  private final ZoneId zoneId;

  private transient DateTimeFormatter dateTimeFormatter;

  public SessionMetricsDateTimeBucketAssigner() {
    this(DEFAULT_FORMAT_STRING);
  }

  public SessionMetricsDateTimeBucketAssigner(String formatString) {
    this(formatString, ZoneId.of("-7"));
  }

  public SessionMetricsDateTimeBucketAssigner(String formatString, ZoneId zoneId) {
    this.formatString = Preconditions.checkNotNull(formatString);
    this.zoneId = Preconditions.checkNotNull(zoneId);
  }

  @Override
  public String getBucketId(SessionMetrics sessionMetrics, Context context) {

    String defaultTsStr;

    if (dateTimeFormatter == null) {
      dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
    }

    defaultTsStr = dateTimeFormatter.format(Instant.ofEpochMilli(
        SojTimestamp.getSojTimestampToUnixTimestamp(sessionMetrics.getSessionStartDt())));

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
    return "SessionMetricsDateTimeBucketAssigner{"
        + "formatString='"
        + formatString
        + '\''
        + ", zoneId="
        + zoneId
        + '}';
  }
}
