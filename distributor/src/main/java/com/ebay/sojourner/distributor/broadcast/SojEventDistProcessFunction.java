package com.ebay.sojourner.distributor.broadcast;

import com.ebay.sojourner.common.model.PageIdTopicMapping;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.model.RheosHeader;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.util.ByteArrayUtils;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.distributor.function.AddTagMapFunction;
import com.ebay.sojourner.distributor.function.CFlagFilterFunction;
import com.ebay.sojourner.distributor.route.Router;
import com.ebay.sojourner.distributor.route.SojEventRouter;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaDeserializer;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaDeserializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ebay.sojourner.common.constant.SojHeaders.DISTRIBUTOR_INGEST_TIMESTAMP;
import static com.ebay.sojourner.common.constant.SojHeaders.REALTIME_PRODUCER_TIMESTAMP;

@Slf4j
public class SojEventDistProcessFunction extends
    BroadcastProcessFunction<RawSojEventWrapper, PageIdTopicMapping, RawSojEventWrapper> {

  private final MapStateDescriptor<Integer, PageIdTopicMapping> stateDescriptor;
  private final int ALL_PAGE = 0;
  private final AddTagMapFunction addTagMapFunction = new AddTagMapFunction();
  private final CFlagFilterFunction cFlagFilterFunction = new CFlagFilterFunction();
  private transient KafkaDeserializer<SojEvent> deserializer;
  private transient KafkaSerializer<SojEvent> serializer;

  private final Router<SojEvent> router;

  // large message monitoring
  private Counter largeMessageSizeCounter;
  private Counter droppedEventCounter;
  private final long maxMessageBytes;
  private final boolean debugMode;
  private static final String LARGE_MESSAGE_SIZE_METRIC_NAME = "large-message-size";
  private static final String DROPPED_EVENT_METRIC_NAME = "dropped-event-count";
  public static final String REMOVE_TAG="experience";
  public static final String EXPM_KW="expm-native-events";
  public static final String EXPC_KW="expc-native-events";

  public SojEventDistProcessFunction(MapStateDescriptor<Integer, PageIdTopicMapping> descriptor,
      List<String> topicConfigs, long maxMessageBytes, boolean debugMode) {
    this.stateDescriptor = descriptor;
    final Map<String, String> topicConfigMap = new HashMap<>();
    if (topicConfigs != null) {
      for (String topicConfig : topicConfigs) {
        String[] configStr = topicConfig.split(":");
        if (configStr.length == 2) {
          topicConfigMap.put(configStr[0], configStr[1]);
        }
      }
    }
    this.router = new SojEventRouter(topicConfigMap);
    this.maxMessageBytes = maxMessageBytes;
    this.debugMode = debugMode;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.deserializer = new AvroKafkaDeserializer<>(SojEvent.class);
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
  public void processElement(RawSojEventWrapper sojEventWrapper, ReadOnlyContext ctx,
      Collector<RawSojEventWrapper> out) throws Exception {
    // deserialize to SojEvent, add tags and filter out `cflags`
    byte[] payload = sojEventWrapper.getPayload();
    SojEvent sojEvent = deserializer.decodeValue(payload);

    // add produceTimestamp and ingestTime into sojHeader
    Map<String, ByteBuffer> sojHeader = sojEvent.getSojHeader();

    Map<String, Long> timestamps = sojEventWrapper.getTimestamps();
    if (timestamps != null) {
      if (timestamps.get(DISTRIBUTOR_INGEST_TIMESTAMP) != null) {
        sojHeader.put(DISTRIBUTOR_INGEST_TIMESTAMP,
            ByteBuffer.wrap(ByteArrayUtils.fromLong(timestamps.get(DISTRIBUTOR_INGEST_TIMESTAMP))));
      }

      if (timestamps.get(REALTIME_PRODUCER_TIMESTAMP) != null) {
        sojHeader.put(REALTIME_PRODUCER_TIMESTAMP,
            ByteBuffer.wrap(ByteArrayUtils.fromLong(timestamps.get(REALTIME_PRODUCER_TIMESTAMP))));
      }
    }
    sojEvent.setSojHeader(sojHeader);

    // filter out cflag events
    if (!cFlagFilterFunction.filter(sojEvent)) {
      return;
    }

    // add tags
    sojEvent = addTagMapFunction.map(sojEvent);

    // add createTimestamp and sentTimestamp into rheosHeader
    RheosHeader rheosHeader = sojEvent.getRheosHeader();
    rheosHeader.setEventCreateTimestamp(System.currentTimeMillis());
    rheosHeader.setEventSentTimestamp(System.currentTimeMillis());
    sojEvent.setRheosHeader(rheosHeader);

    // add eventTimestamp for latency monitoring
    sojEventWrapper.setEventTimestamp(sojEvent.getEventTimestamp());

    // serialize sojEvent after adding tags and set to wrapper
    byte[] value = serializer.encodeValue(sojEvent);

    if (value.length > maxMessageBytes) {
      log.info("message size is more than max message size, need drop");
      droppedEventCounter.inc();
      largeMessageSizeCounter.inc(value.length);
      if (debugMode) {
        log.info(String.format("large message size is %s, payload is %s",
            value.length, sojEvent.toString()));
      }
      return;
    }

    sojEventWrapper.setPayload(value);

    ReadOnlyBroadcastState<Integer, PageIdTopicMapping> broadcastState =
        ctx.getBroadcastState(stateDescriptor);

    // distribute events based on simple pageid/topic mapping regardless event is bot or not
    Integer pageId = sojEvent.getPageId();
    if(!debugMode) {
      PageIdTopicMapping mapping = broadcastState.get(pageId);
      if (mapping != null) {
        for (String topic : mapping.getTopics()) {
          sojEventWrapper.setTopic(topic);
          out.collect(sojEventWrapper);
        }
      }

      // distribute events based on complicated filtering logic, also for bot and non-bot
      Set<String> topics = router.target(sojEvent);
      for (String topic : topics) {
        sojEventWrapper.setTopic(topic);
        if (topic.contains(EXPM_KW)||topic.contains(EXPC_KW)) {
          sojEvent.getApplicationPayload().remove(REMOVE_TAG);
          byte[] valueTmp = serializer.encodeValue(sojEvent);
          sojEventWrapper.setPayload(valueTmp);
        }
        out.collect(sojEventWrapper);
      }

      // for bot sojevents, distribute all events if all_page(0) is set
      if (sojEvent.getBot() != 0 && broadcastState.get(ALL_PAGE) != null) {
        for (String topic : broadcastState.get(ALL_PAGE).getTopics()) {
          sojEventWrapper.setTopic(topic);
          out.collect(sojEventWrapper);
        }
      }
    }else{
      Set<String> topics = router.target(sojEvent);
      for (String topic : topics) {
        sojEventWrapper.setTopic(topic);
        if (topic.contains(EXPM_KW)||topic.contains(EXPC_KW)) {
          sojEvent.getApplicationPayload().remove(REMOVE_TAG);
          byte[] valueTmp = serializer.encodeValue(sojEvent);
          sojEventWrapper.setPayload(valueTmp);
        }
        out.collect(sojEventWrapper);
      }
    }
  }

  @Override
  public void processBroadcastElement(PageIdTopicMapping mapping, Context ctx,
      Collector<RawSojEventWrapper> out) throws Exception {
    log.info("process broadcast pageId topic mapping: {}", mapping);
    BroadcastState<Integer, PageIdTopicMapping> broadcastState =
        ctx.getBroadcastState(stateDescriptor);
    if (mapping.getTopics() == null) {
      // remove topic/pageid from state
      broadcastState.remove(mapping.getPageId());
    } else {
      broadcastState.put(mapping.getPageId(), mapping);
    }
  }
}
