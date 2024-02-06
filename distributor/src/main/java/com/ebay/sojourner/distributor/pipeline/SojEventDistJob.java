package com.ebay.sojourner.distributor.pipeline;

import com.ebay.sojourner.common.model.CustomTopicConfig;
import com.ebay.sojourner.common.model.PageIdTopicMapping;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.distributor.broadcast.SojEventDistProcessFunction;
import com.ebay.sojourner.distributor.function.CustomTopicConfigSourceFunction;
import com.ebay.sojourner.distributor.function.DistPipelineMetricsCollectorProcessFunction;
import com.ebay.sojourner.distributor.schema.deserialize.RawSojEventWrapperDeserializationSchema;
import com.ebay.sojourner.distributor.schema.serialize.RawSojEventWrapperKeySerializerSchema;
import com.ebay.sojourner.distributor.schema.serialize.RawSojEventWrapperValueSerializerSchema;
import com.ebay.sojourner.flink.common.FlinkEnv;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

import static com.ebay.sojourner.common.constant.ConfigProperty.REST_CLIENT_BASE_URL;
import static com.ebay.sojourner.common.constant.ConfigProperty.REST_CLIENT_CONFIG_PROFILE;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

public class SojEventDistJob {

    public static void main(String[] args) throws Exception {

        FlinkEnv flinkEnv = new FlinkEnv(args);
        StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

        // operator uid
        final String UID_KAFKA_SOURCE_EVENT = "kafka-source-event";
        final String UID_CONFIG_SOURCE = "config-source";
        final String UID_DIST = "filter-and-dist";
        final String UID_KAFKA_SINK_EVENT = "kafka-sink-event";
        final String UID_METRICS_COLLECT = "pipeline-metrics-collector";

        // operator name
        final String NAME_KAFKA_SOURCE_EVENT = String.format("Kafka: SojEvent - %s",
                                                             flinkEnv.getSourceKafkaStreamName());
        final String NAME_CONFIG_SOURCE = "PageId Topic Mapping Configs Source";
        final String NAME_DIST = "SojEvent Filter and Distribution";
        final String NAME_KAFKA_SINK_EVENT = String.format("Kafka Sink: SojEvent - %s",
                                                           flinkEnv.getSinkKafkaStreamName());
        final String NAME_METRICS_COLLECT = "Pipeline Metrics Collector";

        // state
        final String STATE_CUSTOM_TOPIC_CONFIG = "custom-topic-config";
        final String STATE_PAGE_ID_TOPIC_MAPPING = "pageId-topic-mapping";

        // config
        final List<String> TOPIC_CONFIG_LIST = flinkEnv.getList("flink.app.dist.topic-config");
        final Long LARGE_MESSAGE_MAX_BYTES = flinkEnv.getLong("flink.app.filter.large-message.max-bytes");

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
                executionEnvironment.fromSource(kafkaSource, noWatermarks(), NAME_KAFKA_SOURCE_EVENT)
                                    .uid(UID_KAFKA_SOURCE_EVENT)
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
                                .process(new SojEventDistProcessFunction(stateDescriptor,
                                                                         TOPIC_CONFIG_LIST,
                                                                         LARGE_MESSAGE_MAX_BYTES))
                                .name(NAME_DIST)
                                .uid(UID_DIST)
                                .setParallelism(flinkEnv.getSourceParallelism());

        // distributor latency monitoring
        sojEventDistStream.process(new DistPipelineMetricsCollectorProcessFunction())
                          .name(NAME_METRICS_COLLECT)
                          .uid(UID_METRICS_COLLECT)
                          .setParallelism(flinkEnv.getSourceParallelism());

        // sink to kafka
        KafkaSink<RawSojEventWrapper> kafkaSink =
                KafkaSink.<RawSojEventWrapper>builder()
                         .setBootstrapServers(flinkEnv.getSinkKafkaBrokers())
                         .setKafkaProducerConfig(flinkEnv.getKafkaProducerProps())
                         .setRecordSerializer(
                                 KafkaRecordSerializationSchema.<RawSojEventWrapper>builder()
                                                               .setTopicSelector(RawSojEventWrapper::getTopic)
                                                               .setKeySerializationSchema(
                                                                       new RawSojEventWrapperKeySerializerSchema())
                                                               .setValueSerializationSchema(
                                                                       new RawSojEventWrapperValueSerializerSchema())
                                                               .build()
                         )
                         .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                         .build();

        sojEventDistStream.sinkTo(kafkaSink)
                          .name(NAME_KAFKA_SINK_EVENT)
                          .uid(UID_KAFKA_SINK_EVENT)
                          .setParallelism(flinkEnv.getSinkParallelism());

        // Submit this job
        flinkEnv.execute(executionEnvironment);
    }
}
