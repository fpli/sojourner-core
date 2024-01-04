package com.ebay.sojourner.distributor.pipeline;

import com.ebay.sojourner.common.model.CustomTopicConfig;
import com.ebay.sojourner.common.model.PageIdTopicMapping;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.distributor.broadcast.SojEventDistProcessFunction;
import com.ebay.sojourner.distributor.function.CustomTopicConfigSourceFunction;
import com.ebay.sojourner.distributor.function.DistPipelineMetricsCollectorProcessFunction;
import com.ebay.sojourner.distributor.schema.RawSojEventWrapperDeserializationSchema;
import com.ebay.sojourner.distributor.schema.RawSojEventWrapperKeySerializerSchema;
import com.ebay.sojourner.distributor.schema.RawSojEventWrapperValueSerializerSchema;
import com.ebay.sojourner.flink.common.FlinkEnv;
import io.ebay.rheos.flink.connector.kafkaha.sink.FlinkKafkaHaProducer;
import io.ebay.rheos.flink.connector.kafkaha.sink.FlinkKafkaHaProducerBuilder;
import io.ebay.rheos.flink.connector.kafkaha.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

import static com.ebay.sojourner.common.constant.ConfigProperty.REST_CLIENT_BASE_URL;
import static com.ebay.sojourner.common.constant.ConfigProperty.REST_CLIENT_CONFIG_PROFILE;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

public class SojEventHADistJob {

    public static void main(String[] args) throws Exception {

        FlinkEnv flinkEnv = new FlinkEnv(args);
        StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

        // operator uid
        final String UID_KAFKA_DATA_SOURCE = "kafka-data-source";
        final String UID_CONFIG_SOURCE = "config-source";
        final String UID_DIST = "filter-and-dist";
        final String UID_KAFKA_DATA_SINK = "kafka-data-sink";
        final String UID_PIPELINE_METRIC = "pipeline-metrics-collector";

        // operator name
        final String NAME_KAFKA_DATA_SOURCE = String.format("Kafka: %s - SojEvent",
                                                            flinkEnv.getSourceKafkaStreamName());
        final String NAME_CONFIG_SOURCE = "PageId Topic Mapping Configs Source";
        final String NAME_DIST = "SojEvent Filter and Distribution";
        final String NAME_KAFKA_DATA_SINK = String.format("Kafka: %s - SojEvent", flinkEnv.getSinkKafkaStreamName());
        final String NAME_PIPELINE_METRIC = "Pipeline Metrics Collector";

        // state
        final String STATE_CUSTOM_TOPIC_CONFIG = "custom-topic-config";
        final String STATE_PAGE_ID_TOPIC_MAPPING = "pageId-topic-mapping";

        // config
        final long LARGE_MESSAGE_MAX_BYTES = flinkEnv.getLong("flink.app.filter.large-message.max-bytes");
        final String RHEOS_HA_SERVICE_URL = flinkEnv.getString("rheos.ha-service-url");
        final String HA_PRODUCER_NAME = flinkEnv.getString("flink.app.dist.ha.producer-name");
        final int HA_PROBE_INTERVAL_MIN = flinkEnv.getInteger("flink.app.dist.ha.probe-interval-min");

        // kafka data source
        KafkaSource<RawSojEventWrapper> kafkaSource =
                KafkaSource.<RawSojEventWrapper>builder()
                           .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                           .setGroupId(flinkEnv.getSourceKafkaGroupId())
                           .setTopics(flinkEnv.getSourceKafkaTopics())
                           .setProperties(flinkEnv.getKafkaConsumerProps())
                           .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                           .setDeserializer(KafkaRecordDeserializationSchema.of(
                                   new RawSojEventWrapperDeserializationSchema()
                           ))
                           .build();

        SingleOutputStreamOperator<RawSojEventWrapper> sourceDataStream =
                executionEnvironment.fromSource(kafkaSource, noWatermarks(), NAME_KAFKA_DATA_SOURCE)
                                    .uid(UID_KAFKA_DATA_SOURCE)
                                    .setParallelism(flinkEnv.getSourceParallelism());

        ListStateDescriptor<CustomTopicConfig> customTopicConfigListStateDescriptor =
                new ListStateDescriptor<>(STATE_CUSTOM_TOPIC_CONFIG, CustomTopicConfig.class);

        DataStream<PageIdTopicMapping> configSourceStream = executionEnvironment
                .addSource(new CustomTopicConfigSourceFunction(flinkEnv.getString(REST_CLIENT_BASE_URL),
                                                               flinkEnv.getString(REST_CLIENT_CONFIG_PROFILE),
                                                               customTopicConfigListStateDescriptor))
                .name(NAME_CONFIG_SOURCE)
                .uid(UID_CONFIG_SOURCE)
                .setParallelism(1);

        MapStateDescriptor<Integer, PageIdTopicMapping> stateDescriptor = new MapStateDescriptor<>(
                STATE_PAGE_ID_TOPIC_MAPPING,
                BasicTypeInfo.INT_TYPE_INFO,
                TypeInformation.of(new TypeHint<PageIdTopicMapping>() {}));

        BroadcastStream<PageIdTopicMapping> broadcastStream = configSourceStream.broadcast(stateDescriptor);

        // regular sojevents based on pageid
        SingleOutputStreamOperator<RawSojEventWrapper> sojEventDistStream =
                sourceDataStream.connect(broadcastStream)
                                .process(new SojEventDistProcessFunction(
                                        stateDescriptor,
                                        null,
                                        LARGE_MESSAGE_MAX_BYTES))
                                .name(NAME_DIST)
                                .uid(UID_DIST)
                                .setParallelism(flinkEnv.getSourceParallelism());

        // distributor latency monitoring
        sojEventDistStream.process(new DistPipelineMetricsCollectorProcessFunction())
                          .name(NAME_PIPELINE_METRIC)
                          .uid(UID_PIPELINE_METRIC)
                          .setParallelism(flinkEnv.getSourceParallelism());

        // sink to kafka
        FlinkKafkaHaProducerBuilder<RawSojEventWrapper> builder = FlinkKafkaHaProducerBuilder.builder();
        Properties kafkaProducerProps = flinkEnv.getKafkaProducerProps();
        kafkaProducerProps.put("producer.name", HA_PRODUCER_NAME);
        FlinkKafkaHaProducer<RawSojEventWrapper> haProducer =
                builder.setRheosHaServiceUrl(RHEOS_HA_SERVICE_URL)
                       .setProducerName(HA_PRODUCER_NAME)
                       .setKafkaProducerConfig(kafkaProducerProps)
                       .setProbeIntervalMinutes(HA_PROBE_INTERVAL_MIN)
                       .setRecordSerializer(
                               KafkaRecordSerializationSchema.<RawSojEventWrapper>builder()
                                                             .setTopicSelector(RawSojEventWrapper::getTopic)
                                                             .setKeySerializationSchema(
                                                                     new RawSojEventWrapperKeySerializerSchema())
                                                             .setValueSerializationSchema(
                                                                     new RawSojEventWrapperValueSerializerSchema())
                                                             .build()
                       ).build();

        sojEventDistStream.addSink(haProducer)
                          .name(NAME_KAFKA_DATA_SINK)
                          .uid(UID_KAFKA_DATA_SINK)
                          .setParallelism(flinkEnv.getSinkParallelism());

        // Submit this job
        flinkEnv.execute(executionEnvironment);
    }
}
