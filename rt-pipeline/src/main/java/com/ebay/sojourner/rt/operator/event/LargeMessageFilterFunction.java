package com.ebay.sojourner.rt.operator.event;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.util.Constants;
import java.net.URLDecoder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

@Slf4j
public class LargeMessageFilterFunction extends RichFilterFunction<RawEvent> {

  private final long MAX_MESSAGE_BYTES;
  private final long MIN_URL_QUERY_STRING_RATIO;
  private final int SUB_URL_QUERY_STRING_LENGTH;
  private final boolean truncateUrlQueryString;
  private final boolean debugMode;
  private Counter largeMessageCounter;
  private Counter largeMessageSizeCounter;
  private Counter largeUrlQueryStringBeforeSubSizeCounter;
  private Counter largeUrlQueryStringAfterSubSizeCounter;
  private Counter droppedEventCounter;
  private Counter urlQueryStringSubStringedEventCounter;
  private static final String LARGE_MESSAGE_METRIC_NAME = "large-message-count";
  private static final String LARGE_MESSAGE_SIZE_METRIC_NAME = "large-message-size";
  private static final String
      LARGE_URL_QUERY_STRING_BEFORE_SUB_SIZE_METRIC_NAME = "large-url-query-string-before-sub-size";
  private static final String
      LARGE_URL_QUERY_STRING_AFTER_SUB_SIZE_METRIC_NAME = "large-url-query-string-after-sub-size";
  private static final String DROPPED_EVENT_METRIC_NAME = "dropped-event-count";
  private static final String URL_QUERY_STRING_SUB_METRIC_NAME = "url-query-string-sub-event-count";

  public LargeMessageFilterFunction(long MAX_MESSAGE_BYTES, long MIN_URL_QUERY_STRING_RATIO,
      int SUB_URL_QUERY_STRING_LENGTH, boolean truncateUrlQueryString, boolean debugMode) {
    this.MAX_MESSAGE_BYTES = MAX_MESSAGE_BYTES;
    this.MIN_URL_QUERY_STRING_RATIO = MIN_URL_QUERY_STRING_RATIO;
    this.SUB_URL_QUERY_STRING_LENGTH = SUB_URL_QUERY_STRING_LENGTH;
    this.truncateUrlQueryString = truncateUrlQueryString;
    this.debugMode = debugMode;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    log.info("truncate urlQueryString is: " + truncateUrlQueryString);
    largeMessageCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter(LARGE_MESSAGE_METRIC_NAME);

    largeMessageSizeCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter(LARGE_MESSAGE_SIZE_METRIC_NAME);

    largeUrlQueryStringBeforeSubSizeCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter(LARGE_URL_QUERY_STRING_BEFORE_SUB_SIZE_METRIC_NAME);

    largeUrlQueryStringAfterSubSizeCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter(LARGE_URL_QUERY_STRING_AFTER_SUB_SIZE_METRIC_NAME);

    droppedEventCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter(DROPPED_EVENT_METRIC_NAME);

    urlQueryStringSubStringedEventCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter(URL_QUERY_STRING_SUB_METRIC_NAME);
  }

  @Override
  public boolean filter(RawEvent rawEvent) throws Exception {

    if (rawEvent.getMessageSize() >= MAX_MESSAGE_BYTES) {
      String urlQueryString = rawEvent.getClientData().getUrlQueryString();
      if (debugMode) {
        log.info(String.format("large message size is %s, message payload is %s",
            rawEvent.getMessageSize(), rawEvent.toString()));
      }
      largeMessageCounter.inc();
      largeMessageSizeCounter.inc(rawEvent.getMessageSize());
      if (truncateUrlQueryString && StringUtils.isNotBlank(urlQueryString) &&
          urlQueryString.length() >= MAX_MESSAGE_BYTES * MIN_URL_QUERY_STRING_RATIO / 100) {
        if (debugMode) {
          log.info(String.format("before sub urlQueryString size is %s, urlQueryString is %s",
              urlQueryString.length(), urlQueryString));
        }
        urlQueryStringSubStringedEventCounter.inc();
        largeUrlQueryStringBeforeSubSizeCounter
            .inc(rawEvent.getClientData().getUrlQueryString().length());
        String finalUrlQueryString = truncateUrlQueryString(urlQueryString);
        rawEvent.getClientData().setUrlQueryString(finalUrlQueryString);
        if (debugMode) {
          log.info(String.format("after sub urlQueryString size is %s, urlQueryString is %s",
              urlQueryString.length(), urlQueryString));
        }
        largeUrlQueryStringAfterSubSizeCounter
            .inc(rawEvent.getClientData().getUrlQueryString().length());
        return true;
      } else {
        if (debugMode) {
          log.info(String.format("urlQueryString size is %s, urlQueryString is %s",
              urlQueryString.length(), urlQueryString));
        }
        log.info("large message size is not caused by large urlQueryString, need drop");
        droppedEventCounter.inc();
        return false;
      }
    }
    return true;
  }

  public String truncateUrlQueryString(String urlQueryString) {
    String subUrlQueryString = urlQueryString.substring(0, SUB_URL_QUERY_STRING_LENGTH);
    int index = subUrlQueryString.lastIndexOf("&");
    if (index <= 0) {
      return "";
    }
    String finalString = subUrlQueryString.substring(0, index);
    try {
      URLDecoder.decode(finalString, "UTF-8");
    } catch (Exception e) {
      finalString = "";
    }
    return finalString;
  }
}
