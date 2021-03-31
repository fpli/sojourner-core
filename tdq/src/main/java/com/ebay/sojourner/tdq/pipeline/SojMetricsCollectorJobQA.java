package com.ebay.sojourner.tdq.pipeline;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.RawEventMetrics;
import com.ebay.sojourner.common.model.SojMetrics;
import com.ebay.sojourner.common.model.TdqConfigMapping;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import com.ebay.sojourner.flink.connector.kafka.schema.RawEventDeserializationSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.RawEventKafkaDeserializationSchemaWrapper;
import com.ebay.sojourner.tdq.broadcast.RawEventProcessFunction;
import com.ebay.sojourner.tdq.function.EmptyMetricFilterFunction;
import com.ebay.sojourner.tdq.function.SojMetricsAgg;
import com.ebay.sojourner.tdq.function.SojMetricsProcessWindowFunction;
import com.ebay.sojourner.tdq.function.TdqConfigSourceFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_FROM_TIMESTAMP;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN;
import static com.ebay.sojourner.flink.common.DataCenter.*;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.*;

public class SojMetricsCollectorJobQA {

    private static final String CONFIG_SOURCE_OP_NAME = "Tdq Config Mapping Source";
    private static final String CONFIG_SOURCE_UID = "tdq-config-mapping-source";
    private static final String CONNECTOR_OP_NAME = "Connector Operator";
    private static final String CONNECTOR_OP_UID = "connector-operator";
    private static final String FILTER_OP_NAME = "Filter Operator";
    private static final String FILTER_OP_UID = "filter-operator";

    //soj Metrcis collector
    public static void main(String[] args) throws Exception {

        // 0.0 Prepare execution environment
        // 0.1 UBI configuration
        // 0.2 Flink configuration
        args = new String[]{"--profile", "qa"};
        final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

        // 1. Rheos Consumer
        // 1.1 Consume RawEvent from Rheos PathFinder topic
        // 1.2 Assign timestamps and emit watermarks.
        SourceDataStreamBuilder<RawEvent> dataStreamBuilder =
                new SourceDataStreamBuilder<>(executionEnvironment);

        DataStream<RawEvent> rawEventDataStreamForRNO = dataStreamBuilder
                .dc(RNO)
                .operatorName(getString(Property.SOURCE_OPERATOR_NAME_RNO))
                .uid(getString(Property.SOURCE_UID_RNO))
                //                .slotGroup(getString(Property.SOURCE_EVENT_RNO_SLOT_SHARE_GROUP))
                .outOfOrderlessInMin(getInteger(FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN))
                .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
                .idleSourceTimeout(getInteger(Property.FLINK_APP_IDLE_SOURCE_TIMEOUT_IN_MIN))
                .build(new RawEventKafkaDeserializationSchemaWrapper(
                        FlinkEnvUtils.getSet(Property.FILTER_GUID_SET),
                        new RawEventDeserializationSchema()));
        DataStream<RawEvent> rawEventDataStreamForSLC = dataStreamBuilder
                .dc(SLC)
                .operatorName(getString(Property.SOURCE_OPERATOR_NAME_SLC))
                .uid(getString(Property.SOURCE_UID_SLC))
                //                .slotGroup(getString(Property.SOURCE_EVENT_SLC_SLOT_SHARE_GROUP))
                .outOfOrderlessInMin(getInteger(FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN))
                .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
                .idleSourceTimeout(getInteger(Property.FLINK_APP_IDLE_SOURCE_TIMEOUT_IN_MIN))
                .build(new RawEventKafkaDeserializationSchemaWrapper(
                        FlinkEnvUtils.getSet(Property.FILTER_GUID_SET),
                        new RawEventDeserializationSchema()));
        DataStream<RawEvent> rawEventDataStreamForLVS = dataStreamBuilder
                .dc(LVS)
                .operatorName(getString(Property.SOURCE_OPERATOR_NAME_LVS))
                .uid(getString(Property.SOURCE_UID_LVS))
                //                .slotGroup(getString(Property.SOURCE_EVENT_LVS_SLOT_SHARE_GROUP))
                .outOfOrderlessInMin(getInteger(FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN))
                .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
                .idleSourceTimeout(getInteger(Property.FLINK_APP_IDLE_SOURCE_TIMEOUT_IN_MIN))
                .build(new RawEventKafkaDeserializationSchemaWrapper(
                        FlinkEnvUtils.getSet(Property.FILTER_GUID_SET),
                        new RawEventDeserializationSchema()));
        DataStream<TdqConfigMapping> mappingSourceStream = executionEnvironment
                .addSource(new TdqConfigSourceFunction(getString(Property.REST_BASE_URL),
                        getLong(Property.REST_CONFIG_PULL_INTERVAL),
                        getString(Property.REST_CONFIG_ENV)))
                .setParallelism(1)
                .name(CONFIG_SOURCE_OP_NAME)
                .uid(CONFIG_SOURCE_UID)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TdqConfigMapping>forBoundedOutOfOrderness(Duration.ofMinutes(0))
                        .withIdleness(Duration.ofSeconds(1)));

        // union ubiEvent from SLC/RNO/LVS
        DataStream<RawEvent> rawEventDataStream = rawEventDataStreamForRNO
                .union(rawEventDataStreamForLVS)
                .union(rawEventDataStreamForSLC);

        MapStateDescriptor<String, TdqConfigMapping> stateDescriptor = new MapStateDescriptor<>(
                "tdqConfigMappingBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<TdqConfigMapping>() {
                }));

        BroadcastStream<TdqConfigMapping> broadcastStream =
                mappingSourceStream.broadcast(stateDescriptor);

        DataStream<RawEventMetrics> rawEventMetricDataStream =
                rawEventDataStream.connect(broadcastStream)
                        .process(new RawEventProcessFunction(stateDescriptor))
                        .name(CONNECTOR_OP_NAME)
                        .uid(CONNECTOR_OP_UID)
                        .setParallelism(getInteger(Property.SOURCE_PARALLELISM));
        DataStream<RawEventMetrics> filteredRawEventMetricsDataStream = rawEventMetricDataStream
                .filter(new EmptyMetricFilterFunction())
                .setParallelism(getInteger(Property.SOURCE_PARALLELISM))
                .name(FILTER_OP_NAME) //add opensession filter name
                .uid(FILTER_OP_UID) //add opensession filter uid;
                ;
        DataStream<SojMetrics> sojMetricsCollectorDataStream =
                filteredRawEventMetricsDataStream
                        .keyBy("guid")
                        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                        .aggregate(new SojMetricsAgg(), new SojMetricsProcessWindowFunction())
                        .setParallelism(getInteger(Property.DEFAULT_PARALLELISM))
                        .name("sojourner metrics collector")
                        .uid("sojourner-metrics-collector");
        sojMetricsCollectorDataStream
                .addSink(new DiscardingSink<>())
                .setParallelism(getInteger(Property.DEFAULT_PARALLELISM))
                .name("sojourner tdq sink")
                .uid("sojourner-tdq-sink");
        // Submit this job
        FlinkEnvUtils.execute(executionEnvironment, getString(Property.FLINK_APP_NAME));
    }
}