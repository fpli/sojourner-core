package com.ebay.sojourner.distributor.function;

import com.ebay.sojourner.common.model.SimpleDistSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.distributor.route.Router;
import com.ebay.sojourner.distributor.route.SojEventRouter;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaSerializer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class SimpleDistSojEventWrapperProcessFunction extends
    ProcessFunction<SojEvent, SimpleDistSojEventWrapper> {

  private final Map<String, String> topicConfigMap = new HashMap<>();
  private final Router<SojEvent> router;
  private transient KafkaSerializer<SojEvent> serializer;
  private final List<String> keyList;

  // large message monitoring
  private Counter largeMessageSizeCounter;
  private Counter droppedEventCounter;
  private final long maxMessageBytes;
  private static final String LARGE_MESSAGE_SIZE_METRIC_NAME = "large-message-size";
  private static final String DROPPED_EVENT_METRIC_NAME = "dropped-event-count";


  public SimpleDistSojEventWrapperProcessFunction(List<String> keyList,
      List<String> topicConfigs, long maxMessageBytes) {
    if (topicConfigs != null) {
      for (String topicConfig : topicConfigs) {
        String[] configStr = topicConfig.split(":");
        if (configStr.length == 2) {
          topicConfigMap.put(configStr[0], configStr[1]);
        }
      }
    }
    this.keyList = keyList;
    this.router = new SojEventRouter(topicConfigMap);
    this.maxMessageBytes = maxMessageBytes;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.serializer = new AvroKafkaSerializer<>(SojEvent.getClassSchema());

    // large message monitoring
    largeMessageSizeCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter(LARGE_MESSAGE_SIZE_METRIC_NAME);

    droppedEventCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter(DROPPED_EVENT_METRIC_NAME);


  }

  @Override
  public void processElement(SojEvent sojEvent, Context ctx,
      Collector<SimpleDistSojEventWrapper> out) throws Exception {

    Set<String> topics = router.target(sojEvent);
    byte[] value = serializer.encodeValue(sojEvent);
    byte[] key = serializer.encodeKey(sojEvent, keyList);

    if (value.length > maxMessageBytes) {
      log.info("message size is more than max message size, need drop");
      droppedEventCounter.inc();
      largeMessageSizeCounter.inc(value.length);
      if (log.isDebugEnabled()) {
        log.debug(String.format("large message size is %s, payload is %s",
            value.length, sojEvent.toString()));
      }
      return;
    }

    for (String topic : topics) {
      SimpleDistSojEventWrapper sojEventWrapper = new SimpleDistSojEventWrapper();
      sojEventWrapper.setGuid(sojEvent.getGuid());
      sojEventWrapper.setColo(sojEvent.getClientData().get("colo"));
      sojEventWrapper.setKey(key);
      sojEventWrapper.setValue(value);
      sojEventWrapper.setTopic(topic);
      out.collect(sojEventWrapper);
    }
  }
}