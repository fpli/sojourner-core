package com.ebay.sojourner.rt.operator.event;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.util.Constants;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static com.ebay.sojourner.cjs.util.ConditionalInvoke.invokeIfNotNull;

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

    private final Set<String> pageIdMonitorList;
    private final Map<String, Consumer<Object>> triggers = new HashMap<>();

    public LargeMessageHandler(long maxMessageBytes, int subUrlQueryStringBytes,
                               boolean truncateUrlQueryString, Set<String> pageIdMonitorList) {
        this.maxMessageBytes = maxMessageBytes;
        this.subUrlQueryStringBytes = subUrlQueryStringBytes;
        this.truncateUrlQueryString = truncateUrlQueryString;
        this.pageIdMonitorList = pageIdMonitorList;
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

        initLargeMsgPerPageMetrics();
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
                invokeTrigger(getPageId(rawEvent), rawEvent);
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
                invokeTrigger(getPageId(rawEvent), rawEvent);
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

    private void initLargeMsgPerPageMetrics() {
        Preconditions.checkNotNull(pageIdMonitorList);

        pageIdMonitorList.forEach(pageId -> {
            val trimmed = StringUtils.trimToEmpty(pageId);
            if (!StringUtils.isNumeric(trimmed)) {
                return;
            }
            try {
                val i = Integer.parseInt(trimmed);
                if (i < 0) {
                    log.warn("Invalid pageId: " + pageId);
                    return;
                }
            } catch (NumberFormatException e) {
                log.warn("Invalid pageId: " + pageId);
                return;
            }

            val largeMsgPerPageCounter =
                    getRuntimeContext()
                            .getMetricGroup()
                            .addGroup(Constants.SOJ_METRIC_GROUP)
                            .counter(DROPPED_EVENT_METRIC_NAME + "." + pageId);

            registerTrigger(pageId, x -> largeMsgPerPageCounter.inc());
        });
    }

    private void registerTrigger(@NonNull String triggerName, @NonNull Consumer<Object> trigger) {
        triggers.put(triggerName, trigger);
    }

    public void invokeTrigger(@NonNull String triggerName, Object param) {
        invokeIfNotNull(triggers.get(triggerName), x -> x.accept(param));
    }

    private static String getPageId(RawEvent event) {
        Map<String, String> map = new HashMap<>();
        map.putAll(event.getSojA());
        map.putAll(event.getSojK());
        map.putAll(event.getSojC());

        String pageId = StringUtils.trimToEmpty(map.get(Constants.P_TAG));
        if (StringUtils.isNumeric(pageId)) {
            return pageId;
        } else {
            return "0";
        }
    }
}
