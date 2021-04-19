package com.ebay.sojourner.flink.connector.kafka.schema;

import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RheosEventDeserializationSchema implements KafkaDeserializationSchema<RheosEvent> {

  private transient RheosEventDeserializer rheosEventDeserializer;

  public RheosEventDeserializationSchema() {
    this.rheosEventDeserializer = new RheosEventDeserializer();
  }

  @Override
  public boolean isEndOfStream(RheosEvent nextElement) {
    return false;
  }

  @Override
  public RheosEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
    if (rheosEventDeserializer == null) {
      rheosEventDeserializer = new RheosEventDeserializer();
    }
    return rheosEventDeserializer.deserialize(null, record.value());
  }

  @Override
  public TypeInformation<RheosEvent> getProducedType() {
    return TypeInformation.of(RheosEvent.class);
  }
}
