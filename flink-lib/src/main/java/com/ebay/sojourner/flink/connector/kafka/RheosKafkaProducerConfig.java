package com.ebay.sojourner.flink.connector.kafka;

import com.ebay.sojourner.common.util.Property;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Data;

@Deprecated
@AllArgsConstructor
@Data
public class RheosKafkaProducerConfig implements Serializable {
  private String rheosServiceUrls;
  private String topic;
  private String schemaSubject;
  private String producerId;
  private Properties kafkaProps;

  public Map<String, Object> toConfigMap() {
    Map<String, Object> map = new HashMap<>((Map) kafkaProps);
    map.put(Property.PRODUCER_ID, producerId);
    map.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrls);
    return map;
  }
}
