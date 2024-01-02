package com.ebay.sojourner.distributor.pipeline;

import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_PARALLELISM_SINK;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_PARALLELISM_SOURCE;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

import com.ebay.sojourner.common.model.RawSojSessionWrapper;
import com.ebay.sojourner.distributor.function.SessionEnhanceMapFunction;
import com.ebay.sojourner.distributor.schema.RawSojSessionWrapperDeserializationSchema;
import com.ebay.sojourner.distributor.schema.RawSojSessionWrapperKeySerializerSchema;
import com.ebay.sojourner.distributor.schema.RawSojSessionWrapperValueSerializerSchema;
import com.ebay.sojourner.flink.common.FlinkEnv;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SojSessionDistJob {

  public static void main(String[] args) throws Exception {

    FlinkEnv flinkEnv = new FlinkEnv(args);
    StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

    // operator uid
    final String UID_KAFKA_DATA_SOURCE = "kafka-data-source";
    final String UID_MAP_ENHANCE = "enhance-map";
    final String UID_KAFKA_DATA_SINK = "kafka-data-sink";

    // operator name
    final String NAME_KAFKA_DATA_SOURCE = String.format("Kafka: %s - SojSession", flinkEnv.getSourceKafkaStreamName());
    final String NAME_MAP_ENHANCE = "SojSession Enhancement";
    final String NAME_KAFKA_DATA_SINK = "Kafka: behavior.pulsar - SojSession";

    // config
    final String DIST_TOPIC = flinkEnv.getString("flink.app.dist.topic");

    // kafka data source
    KafkaSource<RawSojSessionWrapper> kafkaSource =
        KafkaSource.<RawSojSessionWrapper>builder()
                   .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                   .setGroupId(flinkEnv.getSourceKafkaGroupId())
                   .setTopics(flinkEnv.getSourceKafkaTopics())
                   .setProperties(flinkEnv.getKafkaConsumerProps())
                   .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                   .setDeserializer(KafkaRecordDeserializationSchema.of(
                       new RawSojSessionWrapperDeserializationSchema()
                   ))
                   .build();

    SingleOutputStreamOperator<RawSojSessionWrapper> sourceDataStream =
        executionEnvironment.fromSource(kafkaSource, noWatermarks(), NAME_KAFKA_DATA_SOURCE)
                            .uid(UID_KAFKA_DATA_SOURCE)
                            .setParallelism(flinkEnv.getInteger(FLINK_APP_PARALLELISM_SOURCE));

    SingleOutputStreamOperator<RawSojSessionWrapper> mappedDataStream =
        sourceDataStream.map(new SessionEnhanceMapFunction())
                        .name(NAME_MAP_ENHANCE)
                        .uid(UID_MAP_ENHANCE)
                        .setParallelism(flinkEnv.getInteger(FLINK_APP_PARALLELISM_SOURCE));

    // sink to kafka
    KafkaSink<RawSojSessionWrapper> kafkaSink =
        KafkaSink.<RawSojSessionWrapper>builder()
                 .setBootstrapServers(flinkEnv.getSinkKafkaBrokers())
                 .setKafkaProducerConfig(flinkEnv.getKafkaProducerProps())
                 .setRecordSerializer(
                     KafkaRecordSerializationSchema.<RawSojSessionWrapper>builder()
                                                   .setTopic(DIST_TOPIC)
                                                   .setKeySerializationSchema(
                                                       new RawSojSessionWrapperKeySerializerSchema())
                                                   .setValueSerializationSchema(
                                                       new RawSojSessionWrapperValueSerializerSchema())
                                                   .build()
                 )
                 .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                 .build();

    mappedDataStream.sinkTo(kafkaSink)
                    .name(NAME_KAFKA_DATA_SINK)
                    .uid(UID_KAFKA_DATA_SINK)
                    .setParallelism(flinkEnv.getInteger(FLINK_APP_PARALLELISM_SINK));

    // Submit this job
    flinkEnv.execute(executionEnvironment);
  }
}
