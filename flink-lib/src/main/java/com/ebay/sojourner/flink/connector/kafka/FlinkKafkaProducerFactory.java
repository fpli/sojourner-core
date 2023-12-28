package com.ebay.sojourner.flink.connector.kafka;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.flink.connector.kafka.schema.AvroKafkaSerializationSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.RheosKafkaSerializationSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.SojEventKafkaSerializationSchema;
import com.google.common.base.Preconditions;
import io.ebay.rheos.flink.connector.kafkaha.sink.FlinkKafkaHaProducer;
import io.ebay.rheos.flink.connector.kafkaha.sink.FlinkKafkaHaProducerBuilder;
import io.ebay.rheos.flink.connector.kafkaha.sink.KafkaRecordSerializationSchemaBuilder;
import io.ebay.rheos.flink.connector.kafkaha.sink.TopicSelector;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

@Deprecated
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

    public <T> FlinkKafkaHaProducer getHaProducer(String producerId, String rheosUrl,
                                                  int probeIntervalMins,
                                                  SerializationSchema<T> serializationSchema,
                                                  TopicSelector<T> topicSelector) {

        Properties properties = new Properties();
        properties.setProperty("producer.name", producerId);
        properties.putAll(config.getProperties());
        properties.remove(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        KafkaRecordSerializationSchemaBuilder<T> schemaBuilder =
                new KafkaRecordSerializationSchemaBuilder<>();

        FlinkKafkaHaProducerBuilder<T> builder = FlinkKafkaHaProducerBuilder.builder();

        return builder.setKafkaProducerConfig(properties)
                .setProbeIntervalMinutes(probeIntervalMins)
                .setRecordSerializer(
                        schemaBuilder
                                .setPartitioner(new FlinkFixedPartitioner<>())
                                .setTopicSelector(topicSelector)
                                .setValueSerializationSchema(serializationSchema)
                                .build())
                .setProducerName(producerId)
                .setRheosHaServiceUrl(rheosUrl)
                .build();

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

    public FlinkKafkaProducer<SojEvent> getSojEventProducer(String rheosServiceUrl, String topic,
                                                            String subject, String producerId,
                                                            boolean allowDrop) {
        Preconditions.checkNotNull(rheosServiceUrl);
        Preconditions.checkNotNull(topic);
        Preconditions.checkNotNull(subject);
        Preconditions.checkNotNull(producerId);

        RheosKafkaProducerConfig rheosKafkaConfig = new RheosKafkaProducerConfig(
                rheosServiceUrl, topic, subject, producerId, config.getProperties());

        return new SojFlinkKafkaProducer<>(topic,
                new SojEventKafkaSerializationSchema(rheosKafkaConfig),
                config.getProperties(),
                Semantic.AT_LEAST_ONCE, allowDrop);
    }

}
