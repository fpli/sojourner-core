package com.ebay.sojourner.rt.operator.event;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.util.Constants;
import java.net.URLDecoder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

@Slf4j
public class LargeMessageHandler extends RichFlatMapFunction<RawEvent, RawEvent> {

  private final long maxMessageBytes;
  private final int subUrlQueryStringBytes;
  private final boolean truncateUrlQueryString;
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

  public LargeMessageHandler(long maxMessageBytes, int subUrlQueryStringBytes,
      boolean truncateUrlQueryString) {
    this.maxMessageBytes = maxMessageBytes;
    this.subUrlQueryStringBytes = subUrlQueryStringBytes;
    this.truncateUrlQueryString = truncateUrlQueryString;
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
  public void flatMap(RawEvent rawEvent, Collector<RawEvent> collector) throws Exception {

    String urlQueryString = rawEvent.getClientData().getUrlQueryString();
    if (rawEvent.getMessageSize() >= maxMessageBytes) {
      if (log.isDebugEnabled()) {
        log.debug(String.format("large message size is %s, message payload is %s",
            rawEvent.getMessageSize(), rawEvent.toString()));
      }
      largeMessageCounter.inc();
      largeMessageSizeCounter.inc(rawEvent.getMessageSize());
      if (!truncateUrlQueryString) {
        log.info("disable truncate urlQueryString, will drop message directly");
        droppedEventCounter.inc();
        return;
      } else if (urlQueryString != null
          && rawEvent.getMessageSize() - urlQueryString.length()
          > maxMessageBytes - subUrlQueryStringBytes) {
        if (log.isDebugEnabled()) {
          log.debug(String.format("urlQueryString size is %s, urlQueryString is %s",
              urlQueryString.length(), urlQueryString));
        }
        log.info(
            "large message size is not caused by large urlQueryString, will drop message directly");
        droppedEventCounter.inc();
        return;
      } else if (urlQueryString != null) {
        if (log.isDebugEnabled()) {
          log.debug(String.format("before sub urlQueryString size is %s, urlQueryString is %s",
              urlQueryString.length(), urlQueryString));
        }
        urlQueryStringSubStringedEventCounter.inc();
        largeUrlQueryStringBeforeSubSizeCounter
            .inc(rawEvent.getClientData().getUrlQueryString().length());
        String finalUrlQueryString = truncateUrlQueryString(urlQueryString);
        rawEvent.getClientData().setUrlQueryString(finalUrlQueryString);
        if (log.isDebugEnabled()) {
          log.debug(String.format("after sub urlQueryString size is %s, urlQueryString is %s",
              finalUrlQueryString.length(), finalUrlQueryString));
        }
        largeUrlQueryStringAfterSubSizeCounter
            .inc(rawEvent.getClientData().getUrlQueryString().length());
      }
    }
    collector.collect(rawEvent);
  }

  public String truncateUrlQueryString(String urlQueryString) {
    String subUrlQueryString = urlQueryString.substring(0, subUrlQueryStringBytes);
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
