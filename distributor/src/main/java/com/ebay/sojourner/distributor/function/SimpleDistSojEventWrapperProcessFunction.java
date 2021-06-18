package com.ebay.sojourner.distributor.function;

import com.ebay.sojourner.common.model.SimpleDistSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Router;
import com.ebay.sojourner.distributor.route.SojEventRouter;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaSerializer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class SimpleDistSojEventWrapperProcessFunction extends
    ProcessFunction<SojEvent, SimpleDistSojEventWrapper> {

  private final Map<String, String> topicConfigMap = new HashMap<>();
  private final Router<SojEvent> router;
  private transient KafkaSerializer<SojEvent> serializer;
  private final List<String> keyList;

  public SimpleDistSojEventWrapperProcessFunction(List<String> keyList,
      List<String> topicConfigs) {
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
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.serializer = new AvroKafkaSerializer<>(SojEvent.getClassSchema());
  }

  @Override
  public void processElement(SojEvent sojEvent, Context ctx,
      Collector<SimpleDistSojEventWrapper> out) throws Exception {

    Set<String> topics = router.target(sojEvent);
    for (String topic : topics) {
      SimpleDistSojEventWrapper sojEventWrapper = new SimpleDistSojEventWrapper();
      byte[] value = serializer.encodeValue(sojEvent);
      byte[] key = serializer.encodeKey(sojEvent, keyList);
      sojEventWrapper.setGuid(sojEvent.getGuid());
      sojEventWrapper.setColo(sojEvent.getClientData().get("colo"));
      sojEventWrapper.setKey(key);
      sojEventWrapper.setValue(value);
      sojEventWrapper.setTopic(topic);
      out.collect(sojEventWrapper);
    }
  }
}