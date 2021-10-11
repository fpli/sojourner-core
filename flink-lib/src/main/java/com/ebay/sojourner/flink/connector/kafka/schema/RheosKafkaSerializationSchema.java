package com.ebay.sojourner.flink.connector.kafka.schema;

import com.ebay.sojourner.flink.connector.kafka.KafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.RheosAvroKafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.RheosKafkaProducerConfig;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.Nullable;

public class RheosKafkaSerializationSchema<T extends SpecificRecord> implements
    KafkaSerializationSchema<T> {

  private final RheosKafkaProducerConfig rheosKafkaConfig;
  private final List<String> keys;
  private final Class<T> clazz;
  private transient KafkaSerializer<T> rheosKafkaSerializer;

  public RheosKafkaSerializationSchema(RheosKafkaProducerConfig rheosKafkaConfig, Class<T> clazz,
                                       String... keys) {
    this.rheosKafkaConfig = rheosKafkaConfig;
    this.clazz = clazz;
    this.keys = Lists.newArrayList(keys);
    this.rheosKafkaSerializer = new RheosAvroKafkaSerializer<>(rheosKafkaConfig, clazz);
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp) {
    if (rheosKafkaSerializer == null) {
      rheosKafkaSerializer = new RheosAvroKafkaSerializer<>(rheosKafkaConfig, clazz);
    }

    return new ProducerRecord<>(rheosKafkaConfig.getTopic(),
                                rheosKafkaSerializer.encodeKey(element, keys),
                                rheosKafkaSerializer.encodeValue(element));
  }
}
