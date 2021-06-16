package com.ebay.sojourner.distributor.broadcast;

import com.ebay.sojourner.common.model.PageIdTopicMapping;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.function.AddTagMapFunction;
import com.ebay.sojourner.distributor.function.CFlagFilterFunction;
import com.ebay.sojourner.distributor.route.Router;
import com.ebay.sojourner.distributor.route.SojEventRouter;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaDeserializer;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaDeserializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaSerializer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class SojEventDistProcessFunction extends
    BroadcastProcessFunction<RawSojEventWrapper, PageIdTopicMapping, RawSojEventWrapper> {

  private final MapStateDescriptor<Integer, PageIdTopicMapping> stateDescriptor;
  private final int ALL_PAGE = 0;
  private final AddTagMapFunction addTagMapFunction = new AddTagMapFunction();
  private final CFlagFilterFunction cFlagFilterFunction = new CFlagFilterFunction();
  private transient KafkaDeserializer<SojEvent> deserializer;
  private transient KafkaSerializer<SojEvent> serializer;

  private final Map<String, String> topicConfigMap = new HashMap<>();
  private final Router<SojEvent> router;

  public SojEventDistProcessFunction(MapStateDescriptor<Integer, PageIdTopicMapping> descriptor,
                                     List<String> topicConfigs) {
    this.stateDescriptor = descriptor;
    if (topicConfigs != null) {
      for (String topicConfig : topicConfigs) {
        String[] configStr = topicConfig.split(":");
        if (configStr.length == 2) {
          topicConfigMap.put(configStr[0], configStr[1]);
        }
      }
    }
    this.router = new SojEventRouter(topicConfigMap);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.deserializer = new AvroKafkaDeserializer<>(SojEvent.class);
    this.serializer = new AvroKafkaSerializer<>(SojEvent.getClassSchema());
  }

  @Override
  public void processElement(RawSojEventWrapper sojEventWrapper, ReadOnlyContext ctx,
                             Collector<RawSojEventWrapper> out) throws Exception {
    // deserialize to SojEvent, add tags and filter out `cflags`
    byte[] payload = sojEventWrapper.getPayload();
    SojEvent sojEvent = deserializer.decodeValue(payload);

    // filter out cflag events
    if (!cFlagFilterFunction.filter(sojEvent)) {
      return;
    }

    // add tags
    sojEvent = addTagMapFunction.map(sojEvent);

    // serialize sojEvent after adding tags and set to wrapper
    sojEventWrapper.setPayload(serializer.encodeValue(sojEvent));

    ReadOnlyBroadcastState<Integer, PageIdTopicMapping> broadcastState =
        ctx.getBroadcastState(stateDescriptor);

    // distribute events based on pageid regardless event is bot or not
    int pageId = sojEventWrapper.getPageId();
    PageIdTopicMapping mapping = broadcastState.get(pageId);
    if (mapping != null) {
      for (String topic : mapping.getTopics()) {
        sojEventWrapper.setTopic(topic);
        out.collect(sojEventWrapper);
      }
    }

    if (sojEvent.getBot() == 0) {
      // for nonbot sojevents, distribute based on complicated filtering logic
      Set<String> topics = router.target(sojEvent);
      for (String topic : topics) {
        sojEventWrapper.setTopic(topic);
        out.collect(sojEventWrapper);
      }
    } else {
      // for bot sojevents, distribute all events if all_page(0) is set
      if (broadcastState.get(ALL_PAGE) != null) {
        for (String topic : broadcastState.get(ALL_PAGE).getTopics()) {
          sojEventWrapper.setTopic(topic);
          out.collect(sojEventWrapper);
        }
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
