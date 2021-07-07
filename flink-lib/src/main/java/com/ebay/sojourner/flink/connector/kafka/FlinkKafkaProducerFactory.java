package com.ebay.sojourner.flink.connector.kafka;

import com.ebay.sojourner.flink.connector.kafka.schema.AvroKafkaSerializationSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.RheosKafkaSerializationSchema;
import com.google.common.base.Preconditions;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

public class FlinkKafkaProducerFactory {

  private final KafkaProducerConfig config;

  public FlinkKafkaProducerFactory(KafkaProducerConfig config) {
    this.config = config;
  }

  public <T> FlinkKafkaProducer<T> get(String topic, KafkaSerializationSchema<T> serializer) {
    return new FlinkKafkaProducer<>(topic, serializer, config.getProperties(),
        Semantic.AT_LEAST_ONCE);
  }

  public <T extends SpecificRecord> FlinkKafkaProducer<T> get(
      AvroKafkaSerializationSchema<T> serializer) {
    return new FlinkKafkaProducer<>(serializer.defaultTopic, serializer, config.getProperties(),
        Semantic.AT_LEAST_ONCE);
  }

  // Rheos kafka producer
  public <T extends SpecificRecord> FlinkKafkaProducer<T> get(Class<T> clazz,
      String rheosServiceUrls, String topic,
      String subject, String producerId,
      String... keys) {
    Preconditions.checkNotNull(rheosServiceUrls);
    Preconditions.checkNotNull(topic);
    Preconditions.checkNotNull(subject);
    Preconditions.checkNotNull(producerId);

    RheosKafkaProducerConfig rheosKafkaConfig = new RheosKafkaProducerConfig(
        rheosServiceUrls, topic, subject, producerId, config.getProperties());

    return new FlinkKafkaProducer<>(topic,
        new RheosKafkaSerializationSchema<>(rheosKafkaConfig, clazz, keys),
        config.getProperties(),
        Semantic.AT_LEAST_ONCE);
  }

  public <T> FlinkKafkaProducer<T> get(String topic, KafkaSerializationSchema<T> serializer,
      boolean allowDrop) {
    return new SojFlinkKafkaProducer<>(topic, serializer, config.getProperties(),
        Semantic.AT_LEAST_ONCE, allowDrop);
  }

  public <T extends SpecificRecord> FlinkKafkaProducer<T> get(
      AvroKafkaSerializationSchema<T> serializer, boolean allowDrop) {
    return new SojFlinkKafkaProducer<>(serializer.defaultTopic, serializer, config.getProperties(),
        Semantic.AT_LEAST_ONCE, allowDrop);
  }

  // Rheos kafka producer
  public <T extends SpecificRecord> FlinkKafkaProducer<T> get(Class<T> clazz,
      String rheosServiceUrls, String topic,
      String subject, String producerId, boolean allowDrop,
      String... keys) {
    Preconditions.checkNotNull(rheosServiceUrls);
    Preconditions.checkNotNull(topic);
    Preconditions.checkNotNull(subject);
    Preconditions.checkNotNull(producerId);

    RheosKafkaProducerConfig rheosKafkaConfig = new RheosKafkaProducerConfig(
        rheosServiceUrls, topic, subject, producerId, config.getProperties());

    return new SojFlinkKafkaProducer<>(topic,
        new RheosKafkaSerializationSchema<>(rheosKafkaConfig, clazz, keys),
        config.getProperties(),
        Semantic.AT_LEAST_ONCE, allowDrop);
  }

}
