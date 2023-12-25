package com.ebay.sojourner.distributor.pipeline;

import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_PARALLELISM_SINK;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_PARALLELISM_SOURCE;
import static com.ebay.sojourner.common.constant.ConfigProperty.REST_CLIENT_BASE_URL;
import static com.ebay.sojourner.common.constant.ConfigProperty.REST_CLIENT_CONFIG_PROFILE;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

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
import java.util.List;
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

public class SojEventDistJob {

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
    final String NAME_KAFKA_DATA_SOURCE = "Kafka: behavior.totalv3 - SojEvent";
    final String NAME_CONFIG_SOURCE = "PageId Topic Mapping Configs Source";
    final String NAME_DIST = "SojEvent Filter and Distribution";
    final String NAME_KAFKA_DATA_SINK = "Kafka: behavior.pulsar - SojEvent";
    final String NAME_PIPELINE_METRIC = "Pipeline Metrics Collector";

    // state
    final String STATE_CUSTOM_TOPIC_CONFIG = "custom-topic-config";
    final String STATE_PAGE_ID_TOPIC_MAPPING = "pageId-topic-mapping";

    // config
    final List<String> TOPIC_CONFIG_LIST = flinkEnv.getList("flink.app.dist.topic-config");
    final Long LARGE_MESSAGE_MAX_BYTES = flinkEnv.getLong("flink.app.filter.large-message.max-bytes");

    // kafka data source
    KafkaSource<RawSojEventWrapper> kafkaSource =
        KafkaSource.<RawSojEventWrapper>builder()
                   .setBootstrapServers(flinkEnv.getKafkaSourceBrokers())
                   .setGroupId(flinkEnv.getKafkaSourceGroupId())
                   .setTopics(flinkEnv.getKafkaSourceTopics())
                   .setProperties(flinkEnv.getKafkaConsumerProps())
                   .setStartingOffsets(flinkEnv.getKafkaSourceStartingOffsets())
                   //TODO: use new api
                   .setDeserializer(KafkaRecordDeserializationSchema.of(
                       new RawSojEventWrapperDeserializationSchema()
                   ))
                   .build();

    SingleOutputStreamOperator<RawSojEventWrapper> sourceDataStream =
        executionEnvironment.fromSource(kafkaSource, noWatermarks(), NAME_KAFKA_DATA_SOURCE)
                            .uid(UID_KAFKA_DATA_SOURCE)
                            .setParallelism(flinkEnv.getInteger(FLINK_APP_PARALLELISM_SOURCE));

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

    BroadcastStream<PageIdTopicMapping> broadcastStream =
        configSourceStream.broadcast(stateDescriptor);

    // regular sojevents based on pageid
    SingleOutputStreamOperator<RawSojEventWrapper> sojEventDistStream =
        sourceDataStream.connect(broadcastStream)
                        .process(new SojEventDistProcessFunction(
                            stateDescriptor,
                            TOPIC_CONFIG_LIST,
                            LARGE_MESSAGE_MAX_BYTES))
                        .name(NAME_DIST)
                        .uid(UID_DIST)
                        .setParallelism(flinkEnv.getInteger(FLINK_APP_PARALLELISM_SOURCE));

    // distributor latency monitoring
    sojEventDistStream.process(new DistPipelineMetricsCollectorProcessFunction())
                      .name(NAME_PIPELINE_METRIC)
                      .uid(UID_PIPELINE_METRIC)
                      .setParallelism(flinkEnv.getInteger(FLINK_APP_PARALLELISM_SOURCE));

    // sink to kafka
    KafkaSink<RawSojEventWrapper> kafkaSink =
        KafkaSink.<RawSojEventWrapper>builder()
                 .setBootstrapServers(flinkEnv.getKafkaSinkBrokers())
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

    sojEventDistStream.shuffle()
                      .sinkTo(kafkaSink)
                      .name(NAME_KAFKA_DATA_SINK)
                      .uid(UID_KAFKA_DATA_SINK)
                      .setParallelism(flinkEnv.getInteger(FLINK_APP_PARALLELISM_SINK));

    // Submit this job
    flinkEnv.execute(executionEnvironment);
  }
}
