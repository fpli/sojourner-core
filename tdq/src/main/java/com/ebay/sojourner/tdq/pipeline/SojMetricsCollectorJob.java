package com.ebay.sojourner.tdq.pipeline;

import com.ebay.sojourner.common.model.*;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.common.OutputTagConstants;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import com.ebay.sojourner.flink.connector.kafka.schema.RawEventDeserializationSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.RawEventKafkaDeserializationSchemaWrapper;
import com.ebay.sojourner.tdq.broadcast.RawEventProcessFunction;
import com.ebay.sojourner.tdq.function.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_FROM_TIMESTAMP;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN;
import static com.ebay.sojourner.flink.common.DataCenter.*;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.*;

public class SojMetricsCollectorJob {

    private static final String CONFIG_SOURCE_OP_NAME = "Tdq Config Mapping Source";
    private static final String CONFIG_SOURCE_UID = "tdq-config-mapping-source";
    private static final String CONFIG_SOURCE_OP_NAME2 = "Tdq Config Mapping Source2";
    private static final String CONFIG_SOURCE_UID2 = "tdq-config-mapping-source2";
    private static final String CONNECTOR_OP_NAME = "Connector Operator";
    private static final String CONNECTOR_OP_UID = "connector-operator";
    private static final String FILTER_OP_NAME = "Filter Operator";
    private static final String FILTER_OP_UID = "filter-operator";

    //soj Metrcis collector
    public static void main(String[] args) throws Exception {

        // 0.0 Prepare execution environment
        // 0.1 UBI configuration
        // 0.2 Flink configuration
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
                .slotGroup(getString(Property.SOURCE_EVENT_RNO_SLOT_SHARE_GROUP))
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
                .slotGroup(getString(Property.SOURCE_EVENT_SLC_SLOT_SHARE_GROUP))
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
                .slotGroup(getString(Property.SOURCE_EVENT_LVS_SLOT_SHARE_GROUP))
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
                .name(CONFIG_SOURCE_OP_NAME)
                .uid(CONFIG_SOURCE_UID)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TdqConfigMapping>forBoundedOutOfOrderness(Duration.ofMinutes(0))
                        .withIdleness(Duration.ofSeconds(1)))
                .setParallelism(1)
                .name(CONFIG_SOURCE_OP_NAME2)
                .uid(CONFIG_SOURCE_UID2);

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
                        .slotSharingGroup(Property.TDQ_NORMALIZER_SLOT_SHARE_GROUP)
                        .setParallelism(getInteger(Property.TDQ_NORMALIZER_PARALLELISM));
        SingleOutputStreamOperator<SojMetrics> sojMetricsCollectorDataStream =
                rawEventMetricDataStream
                        .keyBy("guid")
                        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                        .aggregate(new SojMetricsAgg(), new SojMetricsProcessWindowFunction())
                        .setParallelism(getInteger(Property.METRIICS_COLLECTOR_PARALLELISM))
                        .slotSharingGroup(Property.METRICS_COLLECTOR_SLOT_SHARE_GROUP)
                        .name("sojourner metrics collector")
                        .uid("sojourner-metrics-collector");
        SingleOutputStreamOperator<SojMetrics> sojMetricsCollectorDataStreamPostAgg
                = sojMetricsCollectorDataStream.keyBy("taskIndex","eventTime")
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new SojMetricsSplitAgg(), new SojMetricsSplitProcessWindowFunction())
                .setParallelism(getInteger(Property.METRIICS_COLLECTOR_PARALLELISM))
                .slotSharingGroup(Property.METRICS_COLLECTOR_SLOT_SHARE_GROUP)
                .name("sojourner metrics collector post agg")
                .uid("sojourner-metrics-collector-post-agg");
        DataStream<TagMissingCntMetrics> tagMissingCntMetricsDataStream
                = sojMetricsCollectorDataStreamPostAgg.getSideOutput(
                OutputTagConstants.TAG_MISSING_CNT_METRICS_OUTPUT_TAG);
        DataStream<TagSumMetrics> tagSumMetricsDataStream = sojMetricsCollectorDataStreamPostAgg
                .getSideOutput(OutputTagConstants.TAG_SUM_METRICS_OUTPUT_TAG);
        DataStream<PageCntMetrics> pageCntMetricsDataStream = sojMetricsCollectorDataStreamPostAgg
                .getSideOutput(OutputTagConstants.PAGE_CNT_METRICS_OUTPUT_TAG);
        DataStream<TransformErrorMetrics> transformErrorMetricsDataStream
                = sojMetricsCollectorDataStreamPostAgg.getSideOutput(
                OutputTagConstants.TRANSFORM_ERROR_METRICS_OUTPUT_TAG);
        DataStream<TotalCntMetrics> totalCntMetricsDataStream
                = sojMetricsCollectorDataStreamPostAgg.getSideOutput(
                OutputTagConstants.TOTAL_CNT_METRICS_OUTPUT_TAG);
        tagMissingCntMetricsDataStream.keyBy("metricName", "eventTime")
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new SojMetricsPostAgg<TagMissingCntMetrics>(TagMissingCntMetrics.class),
                        new SojMetricsFinalProcessWindowFunction<TagMissingCntMetrics>())
                .setParallelism(getInteger(Property.METRIICS_COLLECTOR_FINAL_PARALLELISM))
                .slotSharingGroup(Property.METRICS_COLLECTOR_FINAL_SLOT_SHARE_GROUP)
                .name("sojourner tag missing cnt metrics collector")
                .uid("sojourner-tag-missing-cnt-metrics-collector");
        tagSumMetricsDataStream.keyBy("metricName", "eventTime")
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new SojMetricsPostAgg<TagSumMetrics>(TagSumMetrics.class),
                        new SojMetricsFinalProcessWindowFunction<TagSumMetrics>())
                .setParallelism(getInteger(Property.METRIICS_COLLECTOR_FINAL_PARALLELISM))
                .slotSharingGroup(Property.METRICS_COLLECTOR_FINAL_SLOT_SHARE_GROUP)
                .name("sojourner tag sum metrics collector")
                .uid("sojourner-tag-sum-metrics-collector");
        pageCntMetricsDataStream.keyBy("metricName", "eventTime")
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new SojMetricsPostAgg<PageCntMetrics>(PageCntMetrics.class),
                        new SojMetricsFinalProcessWindowFunction<PageCntMetrics>())
                .setParallelism(getInteger(Property.METRIICS_COLLECTOR_FINAL_PARALLELISM))
                .slotSharingGroup(Property.METRICS_COLLECTOR_FINAL_SLOT_SHARE_GROUP)
                .name("sojourner page cnt metrics collector")
                .uid("sojourner-page-cnt-metrics-collector");
        transformErrorMetricsDataStream.keyBy("metricName", "eventTime")
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(
                        new SojMetricsPostAgg<TransformErrorMetrics>(TransformErrorMetrics.class),
                        new SojMetricsFinalProcessWindowFunction<TransformErrorMetrics>())
                .setParallelism(getInteger(Property.METRIICS_COLLECTOR_FINAL_PARALLELISM))
                .slotSharingGroup(Property.METRICS_COLLECTOR_FINAL_SLOT_SHARE_GROUP)
                .name("sojourner tansform error cnt metrics collector")
                .uid("sojourner-tansform-error-cnt-metrics-collector");

        totalCntMetricsDataStream.keyBy( "eventTime")
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(
                        new SojMetricsPostAgg<TotalCntMetrics>(TotalCntMetrics.class),
                        new SojMetricsFinalProcessWindowFunction<TotalCntMetrics>())
                .setParallelism(getInteger(Property.METRIICS_COLLECTOR_FINAL_PARALLELISM))
                .slotSharingGroup(Property.METRICS_COLLECTOR_FINAL_SLOT_SHARE_GROUP)
                .name("sojourner total cnt metrics collector")
                .uid("sojourner-total-cnt-metrics-collector");
        // Submit this job
        FlinkEnvUtils.execute(executionEnvironment, getString(Property.FLINK_APP_NAME));
    }
}