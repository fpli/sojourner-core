package com.ebay.sojourner.distributor.pipeline;

import static com.ebay.sojourner.common.util.Property.FILTER_PARALLELISM;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_NAME;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SINK_DC;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SINK_KAFKA_TOPIC;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SINK_OP_NAME;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_DC;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_OP_NAME;
import static com.ebay.sojourner.common.util.Property.REST_BASE_URL;
import static com.ebay.sojourner.common.util.Property.REST_CONFIG_PROFILE;
import static com.ebay.sojourner.common.util.Property.REST_CONFIG_PULL_INTERVAL;
import static com.ebay.sojourner.common.util.Property.SINK_KAFKA_PARALLELISM;
import static com.ebay.sojourner.common.util.Property.SOURCE_PARALLELISM;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getList;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getLong;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

import com.ebay.sojourner.common.model.CustomTopicConfig;
import com.ebay.sojourner.common.model.PageIdTopicMapping;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.broadcast.SojEventDistProcessFunction;
import com.ebay.sojourner.distributor.function.AddTagMapFunction;
import com.ebay.sojourner.distributor.function.CFlagFilterFunction;
import com.ebay.sojourner.distributor.function.CustomTopicConfigSourceFunction;
import com.ebay.sojourner.distributor.function.SojEventFilterProcessFunction;
import com.ebay.sojourner.distributor.function.SojEventToRawSojEventWrapperMapFunction;
import com.ebay.sojourner.distributor.schema.RawSojEventWrapperSerializationSchema;
import com.ebay.sojourner.distributor.schema.SojEventDeserializationSchema;
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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SojEventDistJob {
  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    final String DATA_SOURCE_OP_NAME = getString(FLINK_APP_SOURCE_OP_NAME);
    final String DATA_SOURCE_UID = "sojevent-dist-source";
    final String CONFIG_SOURCE_OP_NAME = "PageId Topic Mapping Source";
    final String CONFIG_SOURCE_UID = "pageId-topic-mapping-source";
    final String FILTER_CFLAG_OP_NAME = "Filter out cflag events";
    final String FILTER_CFLAG_UID = "filter-cflag-events";
    final String ADD_TAG_OP_NAME = "Add Tag";
    final String ADD_TAG_OP_UID = "add-tag";
    final String MAP_RAWSOJEVENTWRAPPER_OP_NAME = "SojEvent to RawSojEventWrapper";
    final String MAP_RAWSOJEVENTWRAPPER_OP_UID = "sojevent-to-rawsojeventwrapper";
    final String DIST_OP_NAME = "SojEvent Filter and Distribution";
    final String DIST_UID = "sojevent-filter-and-dist";
    final String SOJ_EVENT_FILTER_OP_NAME = "Filter SojEvent Non-Bot";
    final String SOJ_EVENT_FILTER_OP_UID = "filter-sojevent-non-bot";
    final String SINK_OP_NAME = getString(FLINK_APP_SINK_OP_NAME);
    final String SINK_UID = "sojevent-dist-sink";
    final String FLINK_APP_FILTER_TOPIC_CONFIG_KEY = "flink.app.filter.topic-config";

    SourceDataStreamBuilder<SojEvent> dataStreamBuilder =
        new SourceDataStreamBuilder<>(executionEnvironment);

    DataStream<SojEvent> sojEventSourceDataStream = dataStreamBuilder
        .dc(DataCenter.of(getString(FLINK_APP_SOURCE_DC)))
        .parallelism(getInteger(SOURCE_PARALLELISM))
        .operatorName(DATA_SOURCE_OP_NAME)
        .uid(DATA_SOURCE_UID)
        .build(new SojEventDeserializationSchema());

    DataStream<SojEvent> sojEventFilteredDataStream = sojEventSourceDataStream
        .filter(new CFlagFilterFunction())
        .name(FILTER_CFLAG_OP_NAME)
        .uid(FILTER_CFLAG_UID)
        .setParallelism(getInteger(SOURCE_PARALLELISM));

    DataStream<SojEvent> sojEventDataStream = sojEventFilteredDataStream
        .map(new AddTagMapFunction())
        .name(ADD_TAG_OP_NAME)
        .uid(ADD_TAG_OP_UID)
        .setParallelism(getInteger(SOURCE_PARALLELISM));

    DataStream<RawSojEventWrapper> rawSojEventWrapperDataStream = sojEventDataStream
        .map(new SojEventToRawSojEventWrapperMapFunction())
        .name(MAP_RAWSOJEVENTWRAPPER_OP_NAME)
        .uid(MAP_RAWSOJEVENTWRAPPER_OP_UID)
        .setParallelism(getInteger(SOURCE_PARALLELISM));

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
        rawSojEventWrapperDataStream.connect(broadcastStream)
                                    .process(new SojEventDistProcessFunction(stateDescriptor))
                                    .name(DIST_OP_NAME)
                                    .uid(DIST_UID)
                                    .setParallelism(getInteger(SOURCE_PARALLELISM));

    // special sojevents based on complicated filtering logic
    SingleOutputStreamOperator<RawSojEventWrapper> filteredDataStream = sojEventDataStream
        .process(new SojEventFilterProcessFunction(getList(FLINK_APP_FILTER_TOPIC_CONFIG_KEY)))
        .name(SOJ_EVENT_FILTER_OP_NAME)
        .uid(SOJ_EVENT_FILTER_OP_UID)
        .setParallelism(getInteger(FILTER_PARALLELISM));

    DataStream<RawSojEventWrapper> allDataStream = sojEventDistStream.union(filteredDataStream);

    // sink to kafka
    KafkaProducerConfig config = KafkaProducerConfig.ofDC(getString(FLINK_APP_SINK_DC));
    FlinkKafkaProducerFactory producerFactory = new FlinkKafkaProducerFactory(config);
    allDataStream.addSink(producerFactory.get(getString(FLINK_APP_SINK_KAFKA_TOPIC),
                                              new RawSojEventWrapperSerializationSchema()))
                 .setParallelism(getInteger(SINK_KAFKA_PARALLELISM))
                 .name(SINK_OP_NAME)
                 .uid(SINK_UID);

    // Submit this job
    FlinkEnvUtils.execute(executionEnvironment, getString(FLINK_APP_NAME));
  }
}
