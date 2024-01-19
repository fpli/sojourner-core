package com.ebay.sojourner.flink.function;

import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.util.SojTimestamp;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Slf4j
public class SplitMetricsProcessFunction extends ProcessFunction<SessionMetrics, SessionMetrics> {

  private static final String DATE_FORMAT = "yyyyMMdd";
  private static final String DEFAULT_DATE = "19700101";
  private DateTimeFormatter dateTimeFormatter;
  private OutputTag crossDayOutputTag;
  private OutputTag openOutputTag;

  public SplitMetricsProcessFunction(OutputTag crossDayOutputTag, OutputTag openOutputTag) {
    this.crossDayOutputTag = crossDayOutputTag;
    this.openOutputTag = openOutputTag;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    dateTimeFormatter = dateTimeFormatter.ofPattern(DATE_FORMAT).withZone(ZoneId.systemDefault());
  }

  @Override
  public void processElement(SessionMetrics sessionMetrics, Context context, Collector<SessionMetrics> out)
      throws Exception {

    Long sessionEndTimestamp = System.currentTimeMillis();
    Long sessionStartDt = System.currentTimeMillis();

    try {
      sessionEndTimestamp = SojTimestamp
          .getSojTimestampToUnixTimestamp(sessionMetrics.getAbsEndTimestamp());
      sessionStartDt = SojTimestamp
          .getSojTimestampToUnixTimestamp(sessionMetrics.getSessionStartDt());
    } catch (Exception e) {
      log.warn("session end time is null: " + sessionEndTimestamp);
      log.warn("session start time is null: " + sessionStartDt);
    }

    String sessionEndTimeString = transferLongToDateString(sessionEndTimestamp);
    String sessionStartTimeString = transferLongToDateString(sessionStartDt);

    if (sessionMetrics.getIsOpen()) {
      context.output(openOutputTag, sessionMetrics);
    } else if (sessionStartTimeString.equals(sessionEndTimeString)) {
      out.collect(sessionMetrics);
    } else {
      context.output(crossDayOutputTag, sessionMetrics);
    }
  }

  private String transferLongToDateString(Long time) {

    if (time > 0) {
      String defaultTsStr = dateTimeFormatter.format(Instant.ofEpochMilli(time));
      return defaultTsStr.substring(0, 8);
    } else {
      return DEFAULT_DATE;
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
  }
}
