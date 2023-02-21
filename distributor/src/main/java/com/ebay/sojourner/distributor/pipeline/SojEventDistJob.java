package com.ebay.sojourner.distributor.pipeline;

import static com.ebay.sojourner.common.util.Property.*;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getBoolean;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getList;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getLong;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

import com.ebay.sojourner.common.model.CustomTopicConfig;
import com.ebay.sojourner.common.model.PageIdTopicMapping;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.distributor.broadcast.SojEventDistProcessFunction;
import com.ebay.sojourner.distributor.function.CustomTopicConfigSourceFunction;
import com.ebay.sojourner.distributor.function.DistPipelineMetricsCollectorProcessFunction;
import com.ebay.sojourner.distributor.schema.RawSojEventWrapperDeserializationSchema;
import com.ebay.sojourner.distributor.schema.RawSojEventWrapperSerializationSchema;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.connector.kafka.FlinkKafkaProducerFactory;
import com.ebay.sojourner.flink.connector.kafka.KafkaProducerConfig;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SojEventDistJob {
  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    final String DATA_SOURCE_OP_NAME = getString(FLINK_APP_SOURCE_OP_NAME);
    final String DATA_SOURCE_UID = "sojevent-dist-source";
    final String CONFIG_SOURCE_OP_NAME = "PageId Topic Mapping Configs Source";
    final String CONFIG_SOURCE_UID = "pageId-topic-mapping-source";
    final String DIST_OP_NAME = "SojEvent Filter and Distribution";
    final String DIST_UID = "sojevent-filter-and-dist";
    final String SINK_OP_NAME = getString(FLINK_APP_SINK_OP_NAME);
    final String SINK_UID = "sojevent-dist-sink";
    final String FLINK_APP_DIST_TOPIC_CONFIG_KEY = "flink.app.dist.topic-config";

    SourceDataStreamBuilder<RawSojEventWrapper> dataStreamBuilder =
        new SourceDataStreamBuilder<>(executionEnvironment);

    DataStream<RawSojEventWrapper> sojEventSourceDataStream = dataStreamBuilder
        .dc(DataCenter.of(getString(FLINK_APP_SOURCE_DC)))
        .parallelism(getInteger(SOURCE_PARALLELISM))
        .operatorName(DATA_SOURCE_OP_NAME)
        .uid(DATA_SOURCE_UID)
        .build(new RawSojEventWrapperDeserializationSchema());

    ListStateDescriptor<CustomTopicConfig> listStateDescriptor = new ListStateDescriptor<>(
        "customTopicConfigListState",
        CustomTopicConfig.class);

    DataStream<PageIdTopicMapping> configSourceStream = executionEnvironment
        .addSource(new CustomTopicConfigSourceFunction(getString(REST_BASE_URL),
                                                       getLong(REST_CONFIG_PULL_INTERVAL),
                                                       getString(REST_CONFIG_PROFILE),
                                                       listStateDescriptor))
        .name(CONFIG_SOURCE_OP_NAME)
        .uid(CONFIG_SOURCE_UID)
        .setParallelism(1);

    MapStateDescriptor<Integer, PageIdTopicMapping> stateDescriptor = new MapStateDescriptor<>(
        "pageIdTopicMappingBroadcastState",
        BasicTypeInfo.INT_TYPE_INFO,
        TypeInformation.of(new TypeHint<PageIdTopicMapping>() {}));

    BroadcastStream<PageIdTopicMapping> broadcastStream =
        configSourceStream.broadcast(stateDescriptor);

    // regular sojevents based on pageid
    DataStream<RawSojEventWrapper> sojEventDistStream =
        sojEventSourceDataStream.connect(broadcastStream)
                                .process(new SojEventDistProcessFunction(
                                    stateDescriptor,
                                    getList(FLINK_APP_DIST_TOPIC_CONFIG_KEY),
                                    getLong(MAX_MESSAGE_BYTES),
                                    getBoolean(DEBUG_MODE)))
                                .name(DIST_OP_NAME)
                                .uid(DIST_UID)
                                .setParallelism(getInteger(FILTER_PARALLELISM));

    // distributor latency monitoring
    sojEventDistStream
        .process(new DistPipelineMetricsCollectorProcessFunction(
            FlinkEnvUtils.getInteger(Property.METRIC_WINDOW_SIZE)))
        .setParallelism(getInteger(FILTER_PARALLELISM))
        .name("Pipeline Metrics Collector")
        .uid("pipeline-metrics-collector");

    // sink to kafka
    KafkaProducerConfig config = KafkaProducerConfig.ofDC(getString(FLINK_APP_SINK_DC));
    FlinkKafkaProducerFactory producerFactory = new FlinkKafkaProducerFactory(config);
    sojEventDistStream.addSink(producerFactory.get(getString(FLINK_APP_SINK_KAFKA_TOPIC),
                                                   new RawSojEventWrapperSerializationSchema()))
                      .setParallelism(getInteger(SINK_KAFKA_PARALLELISM))
                      .name(SINK_OP_NAME)
                      .uid(SINK_UID);

    // Submit this job
    FlinkEnvUtils.execute(executionEnvironment, getString(FLINK_APP_NAME));
  }
}
