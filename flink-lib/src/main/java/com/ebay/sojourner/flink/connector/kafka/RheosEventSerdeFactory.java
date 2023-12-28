package com.ebay.sojourner.flink.connector.kafka;

import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import java.util.HashMap;
import java.util.Map;

@Deprecated
public class RheosEventSerdeFactory {

  private static final String RHEOS_SERVICES_URL = FlinkEnvUtils
      .getString(Property.RHEOS_KAFKA_REGISTRY_URL);
  private static RheosEventDeserializer rheosEventHeaderDeserializer;
  private static GenericRecordDomainDataDecoder rheosEventDeserializer;

  public static RheosEventDeserializer getRheosEventHeaderDeserializer() {

    if (rheosEventHeaderDeserializer == null) {
      rheosEventHeaderDeserializer = new RheosEventDeserializer();
    }
    return rheosEventHeaderDeserializer;
  }

  public static GenericRecordDomainDataDecoder getRheosEventDeserializer() {
    return getRheosEventDeserializer(null);
  }

  public static GenericRecordDomainDataDecoder getRheosEventDeserializer(String schemaRegistryUrl) {

    if (rheosEventDeserializer == null) {
      Map<String, Object> config = new HashMap<>();
      if (schemaRegistryUrl != null) {
        config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, schemaRegistryUrl);
      } else {
        config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, RHEOS_SERVICES_URL);
      }
      rheosEventDeserializer = new GenericRecordDomainDataDecoder(config);
    }

    return rheosEventDeserializer;
  }
}
