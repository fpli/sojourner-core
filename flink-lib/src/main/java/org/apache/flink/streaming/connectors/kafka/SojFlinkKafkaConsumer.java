package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * This is a temporary hack to avoid an issue in Flink 1.11.0 (https://issues.apache.org/jira/browse/FLINK-19750).
 * When upgrading to Flink 1.12+, this should not be used.
 */
public class SojFlinkKafkaConsumer<T> extends FlinkKafkaConsumer<T> {

    /**
     * Creates a new Kafka streaming source consumer.
     *
     * @param topic             The name of the topic that should be consumed.
     * @param valueDeserializer The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
     * @param props
     */
    public SojFlinkKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
        super(Collections.singletonList(topic), valueDeserializer, props);
    }

    /**
     * Creates a new Kafka streaming source consumer.
     *
     * <p>This constructor allows passing a {@see KafkaDeserializationSchema} for reading key/value
     * pairs, offsets, and topic names from Kafka.
     *
     * @param topic        The name of the topic that should be consumed.
     * @param deserializer The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
     * @param props
     */
    public SojFlinkKafkaConsumer(String topic, KafkaDeserializationSchema<T> deserializer, Properties props) {
        super(Collections.singletonList(topic), deserializer, props);
    }

    /**
     * Creates a new Kafka streaming source consumer.
     *
     * <p>This constructor allows passing multiple topics to the consumer.
     *
     * @param topics       The Kafka topics to read from.
     * @param deserializer The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
     * @param props
     */
    public SojFlinkKafkaConsumer(List<String> topics, DeserializationSchema<T> deserializer, Properties props) {
        super(topics, new KafkaDeserializationSchemaWrapper<>(deserializer), props);
    }

    /**
     * Creates a new Kafka streaming source consumer.
     *
     * <p>This constructor allows passing multiple topics and a key/value deserialization schema.
     *
     * @param topics       The Kafka topics to read from.
     * @param deserializer The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
     * @param props
     */
    public SojFlinkKafkaConsumer(List<String> topics, KafkaDeserializationSchema<T> deserializer, Properties props) {
        super(topics, deserializer, props);
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        if (getRestoredState() != null) {
            this.deserializer.open(() -> getRuntimeContext().getMetricGroup().addGroup("user"));
        }
    }
}
