package com.ebay.sojourner.distributor.schema;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaDeserializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaDeserializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SojEventDeserializationSchema
    implements KafkaDeserializationSchema<SojEvent> {

  private transient KafkaDeserializer<SojEvent> deserializer;

  public SojEventDeserializationSchema() {
    this.deserializer = new AvroKafkaDeserializer<>(SojEvent.class);;
  }

  @Override
  public boolean isEndOfStream(SojEvent nextElement) {
    return false;
  }

  @Override
  public SojEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
    if (this.deserializer == null) {
      this.deserializer = new AvroKafkaDeserializer<>(SojEvent.class);
    }
    return deserializer.decodeValue(record.value());
  }

  @Override
  public TypeInformation<SojEvent> getProducedType() {
    return TypeInformation.of(SojEvent.class);
  }
}
