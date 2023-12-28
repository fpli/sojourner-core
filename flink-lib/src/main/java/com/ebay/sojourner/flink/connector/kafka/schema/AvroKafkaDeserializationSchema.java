package com.ebay.sojourner.flink.connector.kafka.schema;

import com.ebay.sojourner.flink.connector.kafka.AvroKafkaDeserializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaDeserializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Deprecated
public class AvroKafkaDeserializationSchema<T extends SpecificRecord> implements
    KafkaDeserializationSchema<T> {

  private final Class<T> clazz;
  private transient KafkaDeserializer<T> kafkaDeserializer;

  public AvroKafkaDeserializationSchema(Class<T> clazz) {
    this.clazz = clazz;
    this.kafkaDeserializer = new AvroKafkaDeserializer<>(clazz);
  }

  @Override
  public boolean isEndOfStream(T nextElement) {
    return false;
  }

  @Override
  public T deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
    if (kafkaDeserializer == null) {
      kafkaDeserializer = new AvroKafkaDeserializer<>(clazz);
    }

    return kafkaDeserializer.decodeValue(record.value());
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return TypeInformation.of(clazz);
  }
}
